const net = require("net");
const db = {};

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
      db[key] = value;
      connection.write("+OK\r\n");
    } else if (command === "get") {
      const key = cmdArr[1];
      if (db[key] !== undefined) {
        const value = db[key];
        connection.write(`$${value.length}\r\n${value}\r\n`);
      } else {
        // Null bulk string if key doesn't exist
        connection.write("$-1\r\n");
      }
    }
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
