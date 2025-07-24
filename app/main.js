const net = require("net");
const fs = require("fs");
const path = require("path");
const db = {};
const masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
const EMPTY_RDB_HEX =
  "5245444953303039310a000000ff00000000000000000000b409a26a4aa8657b";
const EMPTY_RDB = Buffer.from(EMPTY_RDB_HEX, "hex");

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
      if (!arr) break;
      handleReplicaCommand(arr);
      offset += bytesRead;
    }
    leftover = leftover.slice(offset);
  }

  function handleReplicaCommand(cmdArr) {
    if (!cmdArr || !cmdArr[0]) return;
    const command = cmdArr[0].toLowerCase();
    // --- handle REPLCONF GETACK * ---
    if (
      command === "replconf" &&
      cmdArr[1] &&
      cmdArr[1].toLowerCase() === "getack"
    ) {
      // Send REPLCONF ACK 0 as RESP array
      masterConnection.write(
        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
      );
      return;
    }
    // --- existing SET logic etc below ---
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

// ==== CHANGES FOR REPLICATION START ====
let replicaSockets = []; // Store multiple replication connections
// ==== CHANGES FOR REPLICATION END ====

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

// Uncomment this block to pass the first stage
server = net.createServer((connection) => {
  // ==== CHANGES FOR REPLICATION START ====
  connection.isReplica = false; // Mark whether this socket is a replica
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
      // When PSYNC happens, this is a replica socket
      connection.isReplica = true; // Mark as replica
      replicaSockets.push(connection); // Add to replicaSockets array
      // Send +FULLRESYNC <replid> 0\r\n
      connection.write(`+FULLRESYNC ${masterReplId} 0\r\n`);
      // Prepare the correct empty RDB file buffer (version 11)
      const emptyRDB = Buffer.from([
        0x52,
        0x45,
        0x44,
        0x49,
        0x53, // REDIS
        0x30,
        0x30,
        0x31,
        0x31, // 0011
        0xff, // End of RDB file
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00, // CRC64 (8 bytes)
      ]);
      connection.write(`$${emptyRDB.length}\r\n`);
      connection.write(emptyRDB);
      // Do NOT send \r\n after the binary file!
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

      db[key] = { value, expiresAt };
      connection.write("+OK\r\n");

      // ==== CHANGES FOR REPLICATION START ====
      // Propagate to all replicas if this is NOT the replica connection
      if (!connection.isReplica && replicaSockets.length > 0) {
        const respCmd = encodeRespArray(cmdArr); // Already has correct casing/args
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
    } else if (command === "replconf") {
      connection.write("+OK\r\n");
      // Handler for SYNC or PSYNC
    }
    // If you add DEL or other write commands, add their propagation as above
  });
  connection.on("error", (err) => {
    console.log("Socket error:", err.message);
  });
  connection.on("close", () => {
    if (connection.isReplica) {
      replicaSockets = replicaSockets.filter((sock) => sock !== connection);
    }
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
