const net = require("net");

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  // Handle connection
  connection.on('data', (data) => {
    const cmdArr = parseRESP(data);

        if (cmdArr && cmdArr[0] && cmdArr[0].toLowerCase() === "echo") {
            const message = cmdArr[1] || "";
            const resp = `$${message.length}\r\n${message}\r\n`;
            connection.write(resp);
        } else {
            connection.write("+PONG\r\n");
        }
  })
});

server.listen(6379, "127.0.0.1");

// RESP parser function
function parseRESP(buffer) {
    const str = buffer.toString();

    if (str[0] !== '*') {
        return null;
    }

    const parts = str.split('\r\n').filter(Boolean);

    let arr = [];
    for (let i = 2; i < parts.length; i += 2) {
        arr.push(parts[i]);
    }

    return arr;
}