const net = require("net");
const fs = require("fs");
const path = require("path");
const db = {};
const masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
const EMPTY_RDB = Buffer.from([
  0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xff, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00,
]);

let masterOffset = 0; // Total number of bytes of write commands propagated
let replicaSockets = []; // Store each replica connection with metadata
let pendingWAITs = []; // Pending WAIT commands (from clients)
let pendingXReads = []; // Pending XREAD BLOCK requests

// Get CLI args
let dir = "";
let dbfilename = "";
let port = 6379; // <-- default port
let role = "master";
let masterHost = null;
let masterPort = null;

const args = process.argv;
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--dir" && i + 1 < args.length) {
    dir = args[i + 1];
  }
  if (args[i] === "--dbfilename" && i + 1 < args.length) {
    dbfilename = args[i + 1];
  }
  if (args[i] === "--port" && i + 1 < args.length) {
    // <-- support --port
    port = parseInt(args[i + 1], 10);
  }
  if (args[i] === "--replicaof" && i + 1 < args.length) {
    role = "slave";
    const [host, portStr] = args[i + 1].split(" ");
    masterHost = host;
    masterPort = parseInt(portStr, 10);
  }
}

// ==== REPLICA MODE: receive and apply commands from master ====
if (role === "slave" && masterHost && masterPort) {
  const masterConnection = net.createConnection(masterPort, masterHost, () => {
    masterConnection.write("*1\r\n$4\r\nPING\r\n");
  });

  let handshakeStep = 0;
  let awaitingRDB = false;
  let rdbBytesExpected = 0;
  let leftover = Buffer.alloc(0); // Buffer for command data

  masterConnection.on("data", (data) => {
    if (handshakeStep === 0) {
      const portStr = port.toString();
      masterConnection.write(
        `*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$${portStr.length}\r\n${portStr}\r\n`
      );
      handshakeStep++;
      return;
    } else if (handshakeStep === 1) {
      masterConnection.write(
        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
      );
      handshakeStep++;
      return;
    } else if (handshakeStep === 2) {
      masterConnection.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
      handshakeStep++;
      return;
    }

    // After handshake: receive RDB, then handle all incoming commands
    if (awaitingRDB) {
      // Still reading RDB file
      if (data.length >= rdbBytesExpected) {
        const afterRDB = data.slice(rdbBytesExpected);
        leftover = Buffer.concat([leftover, afterRDB]);
        awaitingRDB = false;
        processLeftover();
      } else {
        rdbBytesExpected -= data.length;
        // Still need more data for RDB
      }
      return;
    }

    if (!awaitingRDB) {
      const str = data.toString();
      if (str.startsWith("+FULLRESYNC")) {
        // Parse $<length>\r\n then RDB
        const idx = str.indexOf("\r\n$");
        if (idx !== -1) {
          const rest = str.slice(idx + 3);
          const match = rest.match(/^(\d+)\r\n/);
          if (match) {
            rdbBytesExpected = parseInt(match[1], 10);
            awaitingRDB = true;
            const rdbStart = idx + 3 + match[0].length;
            const rdbAvailable = data.slice(rdbStart);
            if (rdbAvailable.length >= rdbBytesExpected) {
              // We have whole RDB, handle what's after
              const afterRDB = rdbAvailable.slice(rdbBytesExpected);
              leftover = Buffer.concat([leftover, afterRDB]);
              awaitingRDB = false;
              processLeftover();
            } else {
              // Wait for the rest
              rdbBytesExpected -= rdbAvailable.length;
            }
            return;
          }
        }
      } else {
        // Already past RDB, this is propagated command data!
        leftover = Buffer.concat([leftover, data]);
        processLeftover();
      }
    }
  });

  masterConnection.on("error", (err) => {
    console.log("Error connecting to master:", err.message);
  });

  function processLeftover() {
    let offset = 0;
    while (offset < leftover.length) {
      const [arr, bytesRead] = tryParseRESP(leftover.slice(offset));
      if (!arr || bytesRead === 0) break;

      const command = arr[0] && arr[0].toLowerCase();

      // Handle REPLCONF GETACK *
      if (
        command === "replconf" &&
        arr[1] &&
        arr[1].toLowerCase() === "getack"
      ) {
        // RESP Array: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n
        const ackResp = `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${
          masterOffset.toString().length
        }\r\n${masterOffset}\r\n`;
        masterConnection.write(ackResp);
        masterOffset += bytesRead; // Only update offset after sending
      } else {
        masterOffset += bytesRead;
        handleReplicaCommand(arr); // Handles SET, etc, silently
      }

      offset += bytesRead;
    }
    leftover = leftover.slice(offset);
  }

  function handleReplicaCommand(cmdArr) {
    if (!cmdArr || !cmdArr[0]) return;
    const command = cmdArr[0].toLowerCase();

    if (command === "set") {
      const key = cmdArr[1];
      const value = cmdArr[2];
      let expiresAt = null;
      if (cmdArr.length >= 5 && cmdArr[3].toLowerCase() === "px") {
        const px = parseInt(cmdArr[4], 10);
        expiresAt = Date.now() + px;
      }
      db[key] = { value, expiresAt };
    }
  }

  // Minimal RESP parser for a single array from Buffer, returns [arr, bytesRead]
  function tryParseRESP(buf) {
    if (buf[0] !== 42) return [null, 0]; // not '*'
    const str = buf.toString();
    const firstLineEnd = str.indexOf("\r\n");
    if (firstLineEnd === -1) return [null, 0];
    const numElems = parseInt(str.slice(1, firstLineEnd), 10);
    let elems = [];
    let cursor = firstLineEnd + 2;
    for (let i = 0; i < numElems; i++) {
      if (buf[cursor] !== 36) return [null, 0]; // not '$'
      const lenLineEnd = buf.indexOf("\r\n", cursor);
      if (lenLineEnd === -1) return [null, 0];
      const len = parseInt(buf.slice(cursor + 1, lenLineEnd).toString(), 10);
      const valStart = lenLineEnd + 2;
      const valEnd = valStart + len;
      if (valEnd + 2 > buf.length) return [null, 0]; // incomplete value
      const val = buf.slice(valStart, valEnd).toString();
      elems.push(val);
      cursor = valEnd + 2;
    }
    return [elems, cursor];
  }
}
// ==== END OF REPLICA MODE CHANGES ====

// === RDB FILE LOADING START ===
// Reads all key-value pairs (string type) from RDB, supports expiries
function loadRDB(filepath) {
  // Don't try to load if filepath is missing, doesn't exist, or is a directory!
  if (
    !filepath ||
    !fs.existsSync(filepath) ||
    !fs.statSync(filepath).isFile()
  ) {
    return;
  }
  const buffer = fs.readFileSync(filepath);
  let offset = 0;

  // Header: REDIS0011 (9 bytes)
  offset += 9;

  // Skip metadata sections (starts with 0xFA)
  while (buffer[offset] === 0xfa) {
    offset++; // skip FA
    // name
    let [name, nameLen] = readRDBString(buffer, offset);
    offset += nameLen;
    // value
    let [val, valLen] = readRDBString(buffer, offset);
    offset += valLen;
  }

  // Scan until 0xFE (start of database section)
  while (offset < buffer.length && buffer[offset] !== 0xfe) {
    offset++;
  }

  // DB section starts with 0xFE
  if (buffer[offset] === 0xfe) {
    offset++;
    // db index (size encoded)
    let [dbIndex, dbLen] = readRDBLength(buffer, offset);
    offset += dbLen;
    // Hash table size info: starts with FB
    if (buffer[offset] === 0xfb) {
      offset++;
      // key-value hash table size
      let [kvSize, kvSizeLen] = readRDBLength(buffer, offset);
      offset += kvSizeLen;
      // expiry hash table size (skip)
      let [expSize, expLen] = readRDBLength(buffer, offset);
      offset += expLen;

      // Only handle string type and expiry
      for (let i = 0; i < kvSize; ++i) {
        let expiresAt = null;

        // Handle optional expiry before type
        if (buffer[offset] === 0xfc) {
          // expiry in ms
          offset++;
          expiresAt = Number(buffer.readBigUInt64LE(offset));
          offset += 8;
        } else if (buffer[offset] === 0xfd) {
          // expiry in s
          offset++;
          expiresAt = buffer.readUInt32LE(offset) * 1000;
          offset += 4;
        }

        let type = buffer[offset++];
        if (type !== 0) continue; // 0 means string type

        let [key, keyLen] = readRDBString(buffer, offset);
        offset += keyLen;
        let [val, valLen] = readRDBString(buffer, offset);
        offset += valLen;
        db[key] = { value: val, expiresAt };
      }
    }
  }
}

// Helper: read size-encoded int
function readRDBLength(buffer, offset) {
  let first = buffer[offset];
  let type = first >> 6;
  if (type === 0) {
    return [first & 0x3f, 1];
  } else if (type === 1) {
    let val = ((first & 0x3f) << 8) | buffer[offset + 1];
    return [val, 2];
  } else if (type === 2) {
    let val =
      (buffer[offset + 1] << 24) |
      (buffer[offset + 2] << 16) |
      (buffer[offset + 3] << 8) |
      buffer[offset + 4];
    return [val, 5];
  } else if (type === 3) {
    return [0, 1];
  }
}

// Helper: read string-encoded value
function readRDBString(buffer, offset) {
  let [strlen, lenlen] = readRDBLength(buffer, offset);
  offset += lenlen;
  let str = buffer.slice(offset, offset + strlen).toString();
  return [str, lenlen + strlen];
}

// Try to load the RDB file only if dir and dbfilename are set!
let rdbPath = "";
if (dir && dbfilename) {
  rdbPath = path.join(dir, dbfilename);
  loadRDB(rdbPath);
  // console.log("Loaded keys from RDB:", Object.keys(db)); // Uncomment for debug
}
// === RDB FILE LOADING END ===

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Helper: is a command a write command?
function isWriteCommand(cmd) {
  // Add more if needed (DEL, etc.)
  return ["set", "del"].includes(cmd);
}

// Helper: encode RESP array from array of strings
function encodeRespArray(arr) {
  let resp = `*${arr.length}\r\n`;
  for (const val of arr) {
    resp += `$${val.length}\r\n${val}\r\n`;
  }
  return resp;
}

// Deep encoder for nested RESP arrays
function encodeRespArrayDeep(arr) {
  let resp = `*${arr.length}\r\n`;
  for (const item of arr) {
    if (Array.isArray(item)) {
      resp += encodeRespArrayDeep(item);
    } else {
      resp += `$${item.length}\r\n${item}\r\n`;
    }
  }
  return resp;
}

function encodeRespInteger(n) {
  return `:${n}\r\n`;
}

// ==== MAIN SERVER STARTS HERE ====
// Uncomment this block to pass the first stage
server = net.createServer((connection) => {
  // ==== CHANGES FOR REPLICATION START ====
  connection.isReplica = false; // Mark whether this socket is a replica
  connection.lastAckOffset = 0; // Used for replicas, tracks last ACK offset
  // ==== CHANGES FOR REPLICATION END ====

  // Handle connection
  connection.on("data", (data) => {
    // LOG what the master receives
    console.log("Master received:", data.toString());

    const cmdArr = parseRESP(data);

    if (!cmdArr || !cmdArr[0]) return;

    const command = cmdArr[0].toLowerCase();

    // ==== CHANGES FOR REPLICATION START ====
    // Detect if this is the replication connection
    if (command === "psync") {
      connection.isReplica = true;
      connection.lastAckOffset = 0;
      replicaSockets.push(connection);

      // 1. Send FULLRESYNC
      connection.write(`+FULLRESYNC ${masterReplId} 0\r\n`);
      // 2. Send empty RDB file as bulk string (version 11)
      connection.write(`$${EMPTY_RDB.length}\r\n`);
      connection.write(EMPTY_RDB);
      // No extra \r\n after this!
      return;
    }

    // ===== HANDLE REPLCONF ACK FROM REPLICA =====
    if (
      connection.isReplica &&
      command === "replconf" &&
      cmdArr[1] &&
      cmdArr[1].toLowerCase() === "ack" &&
      cmdArr[2]
    ) {
      const ackOffset = parseInt(cmdArr[2], 10) || 0;
      connection.lastAckOffset = ackOffset;
      resolveWAITs();
      return;
    }

    // ===== HANDLE REPLCONF GETACK FROM REPLICA/CLIENT =====
    if (
      command === "replconf" &&
      cmdArr[1] &&
      cmdArr[1].toLowerCase() === "getack"
    ) {
      // Respond with *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n
      const offsetStr = masterOffset.toString();
      const resp = `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${offsetStr.length}\r\n${offsetStr}\r\n`;
      connection.write(resp);
      return;
    }

    // ===== ALWAYS REPLY TO OTHER REPLCONF COMMANDS WITH +OK =====
    if (command === "replconf") {
      connection.write("+OK\r\n");
      return;
    }
    // ==== CHANGES FOR REPLICATION END ====

    if (command === "ping") {
      connection.write("+PONG\r\n");
    } else if (command === "echo") {
      const message = cmdArr[1] || "";
      connection.write(`$${message.length}\r\n${message}\r\n`);
    } else if (command === "set") {
      const key = cmdArr[1];
      const value = cmdArr[2];

      // Default: no expiry
      let expiresAt = null;

      // Check for PX (case-insensitive)
      if (cmdArr.length >= 5 && cmdArr[3].toLowerCase() === "px") {
        const px = parseInt(cmdArr[4], 10);
        expiresAt = Date.now() + px;
      }

      db[key] = { value, expiresAt, type: "string" }; // <-- ADD type: "string"
      connection.write("+OK\r\n");

      // ==== CHANGES FOR REPLICATION START ====
      // Propagate to all replicas if this is NOT the replica connection
      if (!connection.isReplica && replicaSockets.length > 0) {
        const respCmd = encodeRespArray(cmdArr); // Already has correct casing/args
        masterOffset += Buffer.byteLength(respCmd, "utf8"); // Track the master replication offset
        // Send to all still-writable replicas
        replicaSockets.forEach((sock) => {
          if (sock.writable) {
            sock.write(respCmd);
          }
        });
      }
      // ==== CHANGES FOR REPLICATION END ====
    } else if (command === "get") {
      const key = cmdArr[1];
      const record = db[key];

      if (record) {
        // If expired, delete and return null
        if (record.expiresAt && Date.now() >= record.expiresAt) {
          delete db[key];
          connection.write("$-1\r\n");
        } else {
          const value = record.value;
          connection.write(`$${value.length}\r\n${value}\r\n`);
        }
      } else {
        // Null bulk string if key doesn't exist
        connection.write("$-1\r\n");
      }
    } else if (command === "xadd") {
      // ==== STREAM SUPPORT START + VALIDATION ====
      const streamKey = cmdArr[1];
      let id = cmdArr[2];

      // Parse XADD id (handle '*', '<ms>-*', or explicit)
      let ms, seq;

      // Fully auto
      if (id === "*") {
        ms = Date.now();
        seq = 0;
        if (
          db[streamKey] &&
          db[streamKey].type === "stream" &&
          db[streamKey].entries.length > 0
        ) {
          const last = db[streamKey].entries[db[streamKey].entries.length - 1];
          const [lastMs, lastSeq] = last.id.split("-").map(Number);
          if (lastMs === ms) {
            seq = lastSeq + 1;
          }
        }
        id = `${ms}-${seq}`;
      }
      // Partially auto
      else if (/^\d+-\*$/.test(id)) {
        ms = Number(id.split("-")[0]);
        if (!db[streamKey] || db[streamKey].entries.length === 0) {
          seq = ms === 0 ? 1 : 0;
        } else {
          let maxSeq = -1;
          for (let i = db[streamKey].entries.length - 1; i >= 0; i--) {
            const [entryMs, entrySeq] = db[streamKey].entries[i].id
              .split("-")
              .map(Number);
            if (entryMs === ms) {
              maxSeq = Math.max(maxSeq, entrySeq);
            }
            if (entryMs < ms) break; // stop searching
          }
          seq = maxSeq >= 0 ? maxSeq + 1 : ms === 0 ? 1 : 0;
        }
        id = `${ms}-${seq}`;
      } else {
        // Explicit
        const parts = id.split("-");
        ms = Number(parts[0]);
        seq = Number(parts[1]);
      }

      // Validate id
      if (!/^\d+-\d+$/.test(id) || ms < 0 || seq < 0) {
        connection.write(
          "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        );
        return;
      }
      if (ms === 0 && seq === 0) {
        connection.write(
          "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        );
        return;
      }
      if (ms === 0 && seq < 1) {
        connection.write(
          "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        );
        return;
      }

      // Prepare field-value pairs
      const pairs = {};
      for (let i = 3; i + 1 < cmdArr.length; i += 2) {
        pairs[cmdArr[i]] = cmdArr[i + 1];
      }

      // Stream creation if needed
      if (!db[streamKey]) {
        db[streamKey] = { type: "stream", entries: [] };
      }

      // Strictly greater than last entry check!
      const entries = db[streamKey].entries;
      if (entries.length > 0) {
        const last = entries[entries.length - 1];
        const [lastMs, lastSeq] = last.id.split("-").map(Number);
        if (ms < lastMs || (ms === lastMs && seq <= lastSeq)) {
          connection.write(
            "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
          );
          return;
        }
      }

      // Add entry
      db[streamKey].entries.push({ id, ...pairs });
      connection.write(`$${id.length}\r\n${id}\r\n`);

      // ==== XREAD BLOCK SUPPORT: wake up pending XREADs ====
      let remaining = [];
      for (let req of pendingXReads) {
        let found = [];
        for (let i = 0; i < req.streams.length; ++i) {
          const k = req.streams[i];
          const lastId = req.ids[i];
          const arr = [];
          if (db[k] && db[k].type === "stream") {
            let [lastMs, lastSeq] = lastId.split("-").map(Number);
            for (const entry of db[k].entries) {
              let [eMs, eSeq] = entry.id.split("-").map(Number);
              if (eMs > lastMs || (eMs === lastMs && eSeq > lastSeq)) {
                let fields = [];
                for (let [kk, vv] of Object.entries(entry))
                  if (kk !== "id") fields.push(kk, vv);
                arr.push([entry.id, fields]);
              }
            }
          }
          if (arr.length) found.push([k, arr]);
        }
        if (found.length) {
          if (req.timer) clearTimeout(req.timer);
          req.conn.write(encodeRespArrayDeep(found));
        } else {
          remaining.push(req); // keep waiting
        }
      }
      pendingXReads = remaining;
      // ==== END XREAD BLOCK SUPPORT ====
    } else if (command === "xrange") {
      // ==== XRANGE SUPPORT START ====
      const streamKey = cmdArr[1];
      let start = cmdArr[2];
      let end = cmdArr[3];

      // Check if stream exists and is a stream type
      if (!db[streamKey] || db[streamKey].type !== "stream") {
        // Return empty array if stream does not exist or is not a stream
        connection.write("*0\r\n");
        return;
      }

      // Parse start and end IDs (support shorthand like "0" for "0-0" and "0-9999999999999999999")
      function parseId(idStr, isEnd) {
        if (idStr === "-") {
          // Minimal possible value for start of stream
          return [Number.MIN_SAFE_INTEGER, Number.MIN_SAFE_INTEGER];
        }
        if (idStr === "+") {
          // Max possible value for end of stream
          return [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER];
        }
        if (idStr.includes("-")) {
          const [ms, seq] = idStr.split("-");
          return [parseInt(ms, 10), parseInt(seq, 10)];
        } else {
          // If only milliseconds, default to 0 for start or MAX_SAFE_INTEGER for end
          if (isEnd) {
            return [parseInt(idStr, 10), Number.MAX_SAFE_INTEGER];
          } else {
            return [parseInt(idStr, 10), 0];
          }
        }
      }

      const [startMs, startSeq] = parseId(start, false);
      const [endMs, endSeq] = parseId(end, true);

      // Filter entries in the inclusive range
      const result = [];
      for (const entry of db[streamKey].entries) {
        const [eMs, eSeq] = entry.id.split("-").map(Number);
        // Compare IDs (start <= entry.id <= end)
        const afterStart =
          eMs > startMs || (eMs === startMs && eSeq >= startSeq);
        const beforeEnd = eMs < endMs || (eMs === endMs && eSeq <= endSeq);
        if (afterStart && beforeEnd) {
          // Convert entry (id, ...fields) to expected RESP array format
          const pairs = [];
          for (const [k, v] of Object.entries(entry)) {
            if (k === "id") continue;
            pairs.push(k, v);
          }
          result.push([entry.id, pairs]);
        }
      }

      connection.write(encodeRespArrayDeep(result));
      // ==== XRANGE SUPPORT END ====
    } else if (command === "xread") {
      // ==== XREAD WITH BLOCK SUPPORT ====
      let blockMs = null;
      let blockIdx = cmdArr.findIndex((x) => x.toLowerCase() === "block");
      let streamsIdx = cmdArr.findIndex((x) => x.toLowerCase() === "streams");
      if (blockIdx !== -1) {
        blockMs = parseInt(cmdArr[blockIdx + 1], 10);
      }
      if (streamsIdx === -1) {
        connection.write("*0\r\n");
        return;
      }
      // Get stream keys and IDs
      const streams = [];
      const ids = [];
      let s = streamsIdx + 1;
      while (s < cmdArr.length && !cmdArr[s].includes("-")) {
        streams.push(cmdArr[s]);
        s++;
      }
      while (s < cmdArr.length) {
        ids.push(cmdArr[s]);
        s++;
      }
      // Find new entries for each stream
      let found = [];
      for (let i = 0; i < streams.length; ++i) {
        const k = streams[i];
        const id = ids[i];
        const arr = [];
        if (db[k] && db[k].type === "stream") {
          let [lastMs, lastSeq] = id.split("-").map(Number);
          for (const entry of db[k].entries) {
            let [eMs, eSeq] = entry.id.split("-").map(Number);
            if (eMs > lastMs || (eMs === lastMs && eSeq > lastSeq)) {
              let fields = [];
              for (let [kk, vv] of Object.entries(entry))
                if (kk !== "id") fields.push(kk, vv);
              arr.push([entry.id, fields]);
            }
          }
        }
        if (arr.length) found.push([k, arr]);
      }
      if (found.length) {
        connection.write(encodeRespArrayDeep(found));
        return;
      }
      if (blockMs === null) {
        // no block param, normal XREAD
        connection.write("*0\r\n");
        return;
      }
      // If blockMs is 0, do not set timeout (block forever)
      let timeout = null;
      if (blockMs > 0) {
        timeout = setTimeout(() => {
          connection.write("$-1\r\n");
          pendingXReads = pendingXReads.filter(
            (obj) => obj.conn !== connection
          );
        }, blockMs);
      }
      // Otherwise, just keep it in pendingXReads until a matching XADD wakes it up
      pendingXReads.push({
        conn: connection,
        streams,
        ids,
        timer: timeout,
      });
      // ==== XREAD BLOCK SUPPORT END ====
    } else if (command === "type") {
      // === TYPE COMMAND SUPPORT (string/none/stream) ===
      const key = cmdArr[1];
      if (db[key]) {
        if (db[key].type === "stream") {
          connection.write("+stream\r\n");
        } else {
          connection.write("+string\r\n");
        }
      } else {
        connection.write("+none\r\n");
      }
    } else if (
      command === "config" &&
      cmdArr[1] &&
      cmdArr[1].toLowerCase() === "get" &&
      cmdArr[2]
    ) {
      const param = cmdArr[2].toLowerCase();
      let value = "";
      if (param === "dir") {
        value = dir;
      } else if (param === "dbfilename") {
        value = dbfilename;
      }
      // RESP array of 2 bulk strings: [param, value]
      connection.write(
        `*2\r\n$${param.length}\r\n${param}\r\n$${value.length}\r\n${value}\r\n`
      );
    } else if (command === "keys") {
      const pattern = cmdArr[1];
      let keys = [];
      if (pattern === "*") {
        keys = Object.keys(db);
      } else if (pattern.endsWith("*")) {
        const prefix = pattern.slice(0, -1);
        keys = Object.keys(db).filter((k) => k.startsWith(prefix));
      } else {
        keys = Object.keys(db).filter((k) => k === pattern);
      }
      let resp = `*${keys.length}\r\n`;
      for (const k of keys) {
        resp += `$${k.length}\r\n${k}\r\n`;
      }
      connection.write(resp);
      // === INFO replication handler START ===
    } else if (
      command === "info" &&
      cmdArr[1] &&
      cmdArr[1].toLowerCase() === "replication"
    ) {
      let lines = [`role:${role}`];
      if (role === "master") {
        lines.push(`master_replid:${masterReplId}`);
        lines.push(`master_repl_offset:0`);
      }
      const infoStr = lines.join("\r\n");
      connection.write(`$${infoStr.length}\r\n${infoStr}\r\n`);
      // === INFO replication handler END ===
    } // ==== REPLCONF GETACK handler for master <--- ADD THIS BLOCK ====
    if (
      command === "replconf" &&
      cmdArr[1] &&
      cmdArr[1].toLowerCase() === "getack"
    ) {
      // Respond with *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n
      const offsetStr = masterOffset.toString();
      const resp = `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${offsetStr.length}\r\n${offsetStr}\r\n`;
      connection.write(resp);
      return;
    } else if (command === "wait") {
      // New: WAIT logic that supports offsets/acks!
      const numReplicas = parseInt(cmdArr[1], 10) || 0;
      const timeout = parseInt(cmdArr[2], 10) || 0;
      handleWAITCommand(connection, numReplicas, timeout);
    }
  });

  connection.on("error", (err) => {
    console.log("Socket error:", err.message);
  });
  connection.on("close", () => {
    if (connection.isReplica) {
      replicaSockets = replicaSockets.filter((sock) => sock !== connection);
      resolveWAITs();
    }
    // Clean up pending XREADs for closed connections
    pendingXReads = pendingXReads.filter((p) => p.conn !== connection);
  });
});

server.listen(port, "127.0.0.1"); // <-- use correct port!

// RESP parser function (used by master/client handlers, not replica stream)
function parseRESP(buffer) {
  const str = buffer.toString();

  if (str[0] !== "*") {
    return null;
  }

  const parts = str.split("\r\n").filter(Boolean);

  let arr = [];
  for (let i = 2; i < parts.length; i += 2) {
    arr.push(parts[i]);
  }

  return arr;
}

// ====== WAIT logic below ======
function handleWAITCommand(clientConn, numReplicas, timeout) {
  const waitOffset = masterOffset;
  let resolved = false;

  // Send REPLCONF GETACK * to all replicas
  replicaSockets.forEach((sock) => {
    if (sock.writable) {
      sock.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
    }
  });

  function countAcks() {
    return replicaSockets.filter((r) => r.lastAckOffset >= waitOffset).length;
  }

  function maybeResolve() {
    if (resolved) return;
    let acked = countAcks();
    if (acked >= numReplicas) {
      resolved = true;
      clientConn.write(encodeRespInteger(acked));
      clearTimeout(timer);
      pendingWAITs = pendingWAITs.filter((w) => w !== waitObj);
    }
  }

  // Immediate resolve if enough already
  if (countAcks() >= numReplicas) {
    clientConn.write(encodeRespInteger(countAcks()));
    return;
  }

  // Else, push pending WAIT
  let timer = setTimeout(() => {
    if (!resolved) {
      let acked = countAcks();
      clientConn.write(encodeRespInteger(acked));
      resolved = true;
      pendingWAITs = pendingWAITs.filter((w) => w !== waitObj);
    }
  }, timeout);

  const waitObj = { waitOffset, numReplicas, clientConn, timer, maybeResolve };
  pendingWAITs.push(waitObj);
}

// Call this function after "any" replica ACK is received
function resolveWAITs() {
  pendingWAITs.forEach((w) => w.maybeResolve());
}

// Fulfill blocked XREADs on this stream
function maybeFulfillBlockedXREADs(streamKey, newEntry) {
  for (let i = 0; i < pendingXReads.length; ++i) {
    let p = pendingXReads[i];
    let idx = p.streams.indexOf(streamKey);
    if (idx === -1) continue;
    let [lastMs, lastSeq] = p.ids[idx].split("-").map(Number);
    let [eMs, eSeq] = newEntry.id.split("-").map(Number);
    if (eMs > lastMs || (eMs === lastMs && eSeq > lastSeq)) {
      // Compose reply just like normal XREAD for this stream only
      let fields = [];
      for (let [k, v] of Object.entries(newEntry))
        if (k !== "id") fields.push(k, v);
      let reply = [[streamKey, [[newEntry.id, fields]]]];
      p.conn.write(encodeRespArrayDeep(reply));
      clearTimeout(p.timer);
      pendingXReads[i] = null;
    }
  }
  // Remove any that were fulfilled
  pendingXReads = pendingXReads.filter(Boolean);
}
