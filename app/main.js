const net = require("net");
const fs = require("fs");
const path = require("path");
const db = {};

// Get CLI args
let dir = "";
let dbfilename = "";

const args = process.argv;
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--dir" && i + 1 < args.length) {
    dir = args[i + 1];
  }
  if (args[i] === "--dbfilename" && i + 1 < args.length) {
    dbfilename = args[i + 1];
  }
}

// === RDB FILE LOADING START ===
// Reads all key-value pairs (string type) from RDB
function loadRDB(filepath) {
  if (!fs.existsSync(filepath)) {
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

      // Only handle string type and no expiry
      for (let i = 0; i < kvSize; ++i) {
        let type = buffer[offset++];
        if (type !== 0) continue; // 0 means string type

        let [key, keyLen] = readRDBString(buffer, offset);
        offset += keyLen;
        let [val, valLen] = readRDBString(buffer, offset);
        offset += valLen;
        db[key] = { value: val, expiresAt: null };
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

// Try to load the RDB file!
const rdbPath = path.join(dir, dbfilename);
loadRDB(rdbPath);
console.log("Loaded keys from RDB:", Object.keys(db)); // <-- For debugging, see what you loaded
// === RDB FILE LOADING END ===

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  // Handle connection
  connection.on("data", (data) => {
    const cmdArr = parseRESP(data);

    if (!cmdArr || !cmdArr[0]) return;

    const command = cmdArr[0].toLowerCase();

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
    }
  });
  connection.on("error", (err) => {
    console.log("Socket error:", err.message);
    // You can ignore or handle as needed, just don't throw
  });
});

server.listen(6379, "127.0.0.1");

// RESP parser function
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
