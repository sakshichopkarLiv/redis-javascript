const net = require("net");
const fs = require("fs");
const path = require("path");

// === IN-MEMORY DATABASE ===
// All data, streams, expiries, etc. live in this object.
const db = {};

// === REPLICATION ID ===
// Used for FULLRESYNC response in replication handshake.
const masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

// Minimal empty RDB binary used for FULLRESYNC bulk transfer.
const EMPTY_RDB = Buffer.from([
  0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xff, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00,
]);

// === REPLICATION / WAIT SUPPORT ===
let masterOffset = 0; // Tracks how many bytes we've sent to replicas.
let replicaSockets = []; // All active replica connections.
let pendingWAITs = []; // All WAIT requests still waiting for ACKs.
let pendingXReads = []; // All XREAD BLOCK requests still pending.

// === CLI ARGUMENTS ===
let dir = "";
let dbfilename = "";
let port = 6379;
let role = "master";
let masterHost = null;
let masterPort = null;

// Parse process arguments for port, file, replication info, etc.
const args = process.argv;
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--dir" && i + 1 < args.length) {
    dir = args[i + 1];
  }
  if (args[i] === "--dbfilename" && i + 1 < args.length) {
    dbfilename = args[i + 1];
  }
  if (args[i] === "--port" && i + 1 < args.length) {
    port = parseInt(args[i + 1], 10);
  }
  if (args[i] === "--replicaof" && i + 1 < args.length) {
    role = "slave";
    const [host, portStr] = args[i + 1].split(" ");
    masterHost = host;
    masterPort = parseInt(portStr, 10);
  }
}

// ==== REPLICA MODE: Connect to Master, Sync Data, Apply Commands ====
// This section only runs if started as a replica/slave.
if (role === "slave" && masterHost && masterPort) {
  const masterConnection = net.createConnection(masterPort, masterHost, () => {
    // Start handshake: send PING
    masterConnection.write("*1\r\n$4\r\nPING\r\n");
  });

  let handshakeStep = 0;
  let awaitingRDB = false; // Are we currently reading RDB data?
  let rdbBytesExpected = 0; // Bytes to expect for RDB
  let leftover = Buffer.alloc(0); // Buffer for command parsing

  masterConnection.on("data", (data) => {
    if (handshakeStep === 0) {
      // Step 1: Send our listening port to master
      const portStr = port.toString();
      masterConnection.write(
        `*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$${portStr.length}\r\n${portStr}\r\n`
      );
      handshakeStep++;
      return;
    } else if (handshakeStep === 1) {
      // Step 2: Send our capabilities (psync2)
      masterConnection.write(
        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
      );
      handshakeStep++;
      return;
    } else if (handshakeStep === 2) {
      // Step 3: Request PSYNC (partial or full)
      masterConnection.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
      handshakeStep++;
      return;
    }

    // === After handshake, receive RDB bulk data (if present) ===
    if (awaitingRDB) {
      if (data.length >= rdbBytesExpected) {
        const afterRDB = data.slice(rdbBytesExpected);
        leftover = Buffer.concat([leftover, afterRDB]);
        awaitingRDB = false;
        processLeftover();
      } else {
        rdbBytesExpected -= data.length;
      }
      return;
    }

    if (!awaitingRDB) {
      // Look for FULLRESYNC response and RDB dump
      const str = data.toString();
      if (str.startsWith("+FULLRESYNC")) {
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
              const afterRDB = rdbAvailable.slice(rdbBytesExpected);
              leftover = Buffer.concat([leftover, afterRDB]);
              awaitingRDB = false;
              processLeftover();
            } else {
              rdbBytesExpected -= rdbAvailable.length;
            }
            return;
          }
        }
      } else {
        leftover = Buffer.concat([leftover, data]);
        processLeftover();
      }
    }
  });

  masterConnection.on("error", (err) => {
    console.log("Error connecting to master:", err.message);
  });

  // RESP parser: Accumulates data, applies each command as soon as possible.
  function processLeftover() {
    let offset = 0;
    while (offset < leftover.length) {
      const [arr, bytesRead] = tryParseRESP(leftover.slice(offset));
      if (!arr || bytesRead === 0) break;

      const command = arr[0] && arr[0].toLowerCase();

      // Special handling for REPLCONF GETACK *: respond with our offset
      if (
        command === "replconf" &&
        arr[1] &&
        arr[1].toLowerCase() === "getack"
      ) {
        const ackResp = `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${
          masterOffset.toString().length
        }\r\n${masterOffset}\r\n`;
        masterConnection.write(ackResp);
        masterOffset += bytesRead;
      } else {
        masterOffset += bytesRead;
        handleReplicaCommand(arr);
      }
      offset += bytesRead;
    }
    leftover = leftover.slice(offset);
  }

  // Actually applies SET/etc to our local DB (on the replica)
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
      db[key] = { value, expiresAt, type: "string" };
    }
    // Add DEL/XADD here if supporting on replica.
  }

  // Minimal RESP array parser: returns [array, bytesRead]
  function tryParseRESP(buf) {
    if (!buf.length || buf[0] !== 42) return [null, 0]; // 42 is '*'
    let str = buf.toString();
    let firstLineEnd = str.indexOf("\r\n");
    if (firstLineEnd === -1) return [null, 0];
    let numElems = parseInt(str.slice(1, firstLineEnd), 10);
    let elems = [];
    let cursor = firstLineEnd + 2;
    for (let i = 0; i < numElems; i++) {
      if (buf[cursor] !== 36) return [null, 0]; // 36 is '$'
      let lenLineEnd = buf.indexOf("\r\n", cursor);
      if (lenLineEnd === -1) return [null, 0];
      let len = parseInt(buf.slice(cursor + 1, lenLineEnd).toString(), 10);
      let valStart = lenLineEnd + 2;
      let valEnd = valStart + len;
      if (valEnd + 2 > buf.length) return [null, 0];
      let val = buf.slice(valStart, valEnd).toString();
      elems.push(val);
      cursor = valEnd + 2;
    }
    return [elems, cursor];
  }
}
// ==== END OF REPLICA MODE ====

// === RDB FILE LOADING SUPPORT ===
// Loads simple key-value pairs (and expiries) from an RDB file on disk.
function loadRDB(filepath) {
  if (
    !filepath ||
    !fs.existsSync(filepath) ||
    !fs.statSync(filepath).isFile()
  ) {
    return;
  }
  const buffer = fs.readFileSync(filepath);
  let offset = 0;
  // Skip header
  offset += 9;
  while (buffer[offset] === 0xfa) {
    offset++;
    let [name, nameLen] = readRDBString(buffer, offset);
    offset += nameLen;
    let [val, valLen] = readRDBString(buffer, offset);
    offset += valLen;
  }
  while (offset < buffer.length && buffer[offset] !== 0xfe) {
    offset++;
  }
  if (buffer[offset] === 0xfe) {
    offset++;
    let [dbIndex, dbLen] = readRDBLength(buffer, offset);
    offset += dbLen;
    if (buffer[offset] === 0xfb) {
      offset++;
      let [kvSize, kvSizeLen] = readRDBLength(buffer, offset);
      offset += kvSizeLen;
      let [expSize, expLen] = readRDBLength(buffer, offset);
      offset += expLen;
      for (let i = 0; i < kvSize; ++i) {
        let expiresAt = null;
        if (buffer[offset] === 0xfc) {
          offset++;
          expiresAt = Number(buffer.readBigUInt64LE(offset));
          offset += 8;
        } else if (buffer[offset] === 0xfd) {
          offset++;
          expiresAt = buffer.readUInt32LE(offset) * 1000;
          offset += 4;
        }
        let type = buffer[offset++];
        if (type !== 0) continue;
        let [key, keyLen] = readRDBString(buffer, offset);
        offset += keyLen;
        let [val, valLen] = readRDBString(buffer, offset);
        offset += valLen;
        db[key] = { value: val, expiresAt, type: "string" };
      }
    }
  }
}

// Helpers to read Redis RDB length-encoded types and strings.
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

function readRDBString(buffer, offset) {
  let [strlen, lenlen] = readRDBLength(buffer, offset);
  offset += lenlen;
  let str = buffer.slice(offset, offset + strlen).toString();
  return [str, lenlen + strlen];
}

// If --dir and --dbfilename are set, load from disk on startup.
let rdbPath = "";
if (dir && dbfilename) {
  rdbPath = path.join(dir, dbfilename);
  loadRDB(rdbPath);
}

// Print statements are visible in the test logs.
console.log("Logs from your program will appear here!");

// === RESP ENCODERS (used everywhere) ===
function encodeRespArray(arr) {
  let resp = `*${arr.length}\r\n`;
  for (const val of arr) {
    resp += `$${val.length}\r\n${val}\r\n`;
  }
  return resp;
}
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

// === MAIN TCP SERVER: All Client/Replica Logic ===
server = net.createServer((connection) => {
  connection.isReplica = false;
  connection.lastAckOffset = 0;

  connection.on("data", (data) => {
    // Debug print of raw command received
    console.log("Master received:", data.toString());

    const cmdArr = parseRESP(data);
    if (!cmdArr || !cmdArr[0]) return;

    const command = cmdArr[0].toLowerCase();

    // ==== REPLICATION HANDSHAKE ====
    if (command === "psync") {
      connection.isReplica = true;
      connection.lastAckOffset = 0;
      replicaSockets.push(connection);
      // Send FULLRESYNC and then empty RDB bulk string.
      connection.write(`+FULLRESYNC ${masterReplId} 0\r\n`);
      connection.write(`$${EMPTY_RDB.length}\r\n`);
      connection.write(EMPTY_RDB);
      return;
    }

    // Handle REPLCONF ACK reply from replica (WAIT support)
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
    // Always respond to REPLCONF commands with +OK
    if (command === "replconf") {
      connection.write("+OK\r\n");
      return;
    }

    // ==== MAIN COMMANDS (SET, GET, INCR, etc.) ====
    if (command === "ping") {
      connection.write("+PONG\r\n");
    } else if (command === "echo") {
      const message = cmdArr[1] || "";
      connection.write(`$${message.length}\r\n${message}\r\n`);
    } else if (command === "set") {
      // Standard SET with optional PX
      const key = cmdArr[1];
      const value = cmdArr[2];
      let expiresAt = null;
      if (cmdArr.length >= 5 && cmdArr[3].toLowerCase() === "px") {
        const px = parseInt(cmdArr[4], 10);
        expiresAt = Date.now() + px;
      }
      db[key] = { value, expiresAt, type: "string" };
      connection.write("+OK\r\n");

      // Propagate to replicas if not from a replica
      if (!connection.isReplica && replicaSockets.length > 0) {
        const respCmd = encodeRespArray(cmdArr);
        masterOffset += Buffer.byteLength(respCmd, "utf8");
        replicaSockets.forEach((sock) => {
          if (sock.writable) {
            sock.write(respCmd);
          }
        });
      }
    } else if (command === "multi") {
      // Respond to MULTI command with OK as per Redis protocol
      connection.write("+OK\r\n");
    } else if (command === "get") {
      const key = cmdArr[1];
      const record = db[key];
      if (record) {
        if (record.expiresAt && Date.now() >= record.expiresAt) {
          delete db[key];
          connection.write("$-1\r\n");
        } else {
          const value = record.value;
          connection.write(`$${value.length}\r\n${value}\r\n`);
        }
      } else {
        connection.write("$-1\r\n");
      }
    } else if (command === "incr") {
      const key = cmdArr[1];
      if (db[key] === undefined) {
        // Key does not exist: set to 1
        db[key] = { value: "1", type: "string" };
        connection.write(encodeRespInteger(1));
      } else if (db[key].type === "string" && /^-?\d+$/.test(db[key].value)) {
        // Key exists and value is integer: increment
        let num = parseInt(db[key].value, 10);
        num += 1;
        db[key].value = num.toString();
        connection.write(encodeRespInteger(num));
      } else {
        // Key exists but value is NOT an integer
        connection.write("-ERR value is not an integer or out of range\r\n");
      }
    } else if (command === "xadd") {
      // XADD command: Add to stream, handle IDs (full/partial/auto), error checks
      const streamKey = cmdArr[1];
      let id = cmdArr[2];
      let ms, seq;

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
      } else if (/^\d+-\*$/.test(id)) {
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
            if (entryMs < ms) break;
          }
          seq = maxSeq >= 0 ? maxSeq + 1 : ms === 0 ? 1 : 0;
        }
        id = `${ms}-${seq}`;
      } else {
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
      if (!db[streamKey]) {
        db[streamKey] = { type: "stream", entries: [] };
      }
      // Ensure entry ID is strictly greater
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
      db[streamKey].entries.push({ id, ...pairs });
      connection.write(`$${id.length}\r\n${id}\r\n`);

      // XREAD BLOCK support: wake up blocked clients
      let remaining = [];
      for (let req of pendingXReads) {
        let found = [];
        for (let i = 0; i < req.streams.length; ++i) {
          const k = req.streams[i];
          let id = req.ids[i];
          if (id === "$") {
            if (db[k] && db[k].type === "stream" && db[k].entries.length > 0) {
              id = db[k].entries[db[k].entries.length - 1].id;
            } else {
              id = "0-0";
            }
          }
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
          if (req.timer) clearTimeout(req.timer);
          req.conn.write(encodeRespArrayDeep(found));
        } else {
          remaining.push(req);
        }
      }
      pendingXReads = remaining;
    } else if (command === "xrange") {
      // XRANGE: Return a range of stream entries.
      const streamKey = cmdArr[1];
      let start = cmdArr[2];
      let end = cmdArr[3];
      if (!db[streamKey] || db[streamKey].type !== "stream") {
        connection.write("*0\r\n");
        return;
      }
      function parseId(idStr, isEnd) {
        if (idStr === "-") {
          return [Number.MIN_SAFE_INTEGER, Number.MIN_SAFE_INTEGER];
        }
        if (idStr === "+") {
          return [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER];
        }
        if (idStr.includes("-")) {
          const [ms, seq] = idStr.split("-");
          return [parseInt(ms, 10), parseInt(seq, 10)];
        } else {
          if (isEnd) {
            return [parseInt(idStr, 10), Number.MAX_SAFE_INTEGER];
          } else {
            return [parseInt(idStr, 10), 0];
          }
        }
      }
      const [startMs, startSeq] = parseId(start, false);
      const [endMs, endSeq] = parseId(end, true);
      const result = [];
      for (const entry of db[streamKey].entries) {
        const [eMs, eSeq] = entry.id.split("-").map(Number);
        const afterStart =
          eMs > startMs || (eMs === startMs && eSeq >= startSeq);
        const beforeEnd = eMs < endMs || (eMs === endMs && eSeq <= endSeq);
        if (afterStart && beforeEnd) {
          const pairs = [];
          for (const [k, v] of Object.entries(entry)) {
            if (k === "id") continue;
            pairs.push(k, v);
          }
          result.push([entry.id, pairs]);
        }
      }
      connection.write(encodeRespArrayDeep(result));
    } else if (command === "xread") {
      // XREAD with BLOCK: blocks until stream gets new messages or timeout.
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
      const streams = [];
      const ids = [];
      let s = streamsIdx + 1;
      while (
        s < cmdArr.length &&
        !cmdArr[s].includes("-") &&
        cmdArr[s] !== "$"
      ) {
        streams.push(cmdArr[s]);
        s++;
      }
      while (s < cmdArr.length) {
        ids.push(cmdArr[s]);
        s++;
      }
      const resolvedIds = [];
      for (let i = 0; i < streams.length; ++i) {
        const key = streams[i];
        const reqId = ids[i];
        if (reqId === "$") {
          if (
            db[key] &&
            db[key].type === "stream" &&
            db[key].entries.length > 0
          ) {
            const lastEntry = db[key].entries[db[key].entries.length - 1];
            resolvedIds.push(lastEntry.id);
          } else {
            resolvedIds.push("0-0");
          }
        } else {
          resolvedIds.push(reqId);
        }
      }
      let found = [];
      for (let i = 0; i < streams.length; ++i) {
        const k = streams[i];
        const id = resolvedIds[i];
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
        connection.write("*0\r\n");
        return;
      }
      let timeout = null;
      if (blockMs > 0) {
        timeout = setTimeout(() => {
          connection.write("$-1\r\n");
          pendingXReads = pendingXReads.filter(
            (obj) => obj.conn !== connection
          );
        }, blockMs);
      }
      pendingXReads.push({
        conn: connection,
        streams,
        ids: resolvedIds,
        timer: timeout,
      });
    } else if (command === "type") {
      // TYPE command: tells if value is string/stream/none
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
      // CONFIG GET dir/dbfilename
      const param = cmdArr[2].toLowerCase();
      let value = "";
      if (param === "dir") value = dir;
      else if (param === "dbfilename") value = dbfilename;
      connection.write(
        `*2\r\n$${param.length}\r\n${param}\r\n$${value.length}\r\n${value}\r\n`
      );
    } else if (command === "keys") {
      // KEYS * support (star and prefix match)
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
    } else if (
      command === "info" &&
      cmdArr[1] &&
      cmdArr[1].toLowerCase() === "replication"
    ) {
      // INFO replication: returns replication role/info
      let lines = [`role:${role}`];
      if (role === "master") {
        lines.push(`master_replid:${masterReplId}`);
        lines.push(`master_repl_offset:0`);
      }
      const infoStr = lines.join("\r\n");
      connection.write(`$${infoStr.length}\r\n${infoStr}\r\n`);
    }
    // REPLCONF GETACK * (ask replicas to send their offsets for WAIT)
    if (
      command === "replconf" &&
      cmdArr[1] &&
      cmdArr[1].toLowerCase() === "getack"
    ) {
      const offsetStr = masterOffset.toString();
      const resp = `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${offsetStr.length}\r\n${offsetStr}\r\n`;
      connection.write(resp);
      return;
    }
    // WAIT command (replication): blocks until N replicas ACK offset or timeout
    if (command === "wait") {
      const numReplicas = parseInt(cmdArr[1], 10) || 0;
      const timeout = parseInt(cmdArr[2], 10) || 0;
      handleWAITCommand(connection, numReplicas, timeout);
    }
  });

  // On disconnect/error, clean up replica/socket state
  connection.on("error", (err) => {
    console.log("Socket error:", err.message);
  });
  connection.on("close", () => {
    if (connection.isReplica) {
      replicaSockets = replicaSockets.filter((sock) => sock !== connection);
      resolveWAITs();
    }
    pendingXReads = pendingXReads.filter((p) => p.conn !== connection);
  });
});

server.listen(port, "127.0.0.1");

// Parses RESP array into JS array (e.g. ["SET", "foo", "bar"])
function parseRESP(buffer) {
  const str = buffer.toString();
  if (str[0] !== "*") return null;
  const parts = str.split("\r\n").filter(Boolean);
  let arr = [];
  for (let i = 2; i < parts.length; i += 2) {
    arr.push(parts[i]);
  }
  return arr;
}

// WAIT: Register/resolve WAIT requests when enough replicas have acked.
function handleWAITCommand(clientConn, numReplicas, timeout) {
  const waitOffset = masterOffset;
  let resolved = false;
  // Ask all replicas for their latest ACK
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
  if (countAcks() >= numReplicas) {
    clientConn.write(encodeRespInteger(countAcks()));
    return;
  }
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
function resolveWAITs() {
  pendingWAITs.forEach((w) => w.maybeResolve());
}
