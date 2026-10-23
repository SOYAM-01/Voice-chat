// WebSocket relay for group voice chat. Works on free hosts (Render, Koyeb, etc).
// Packet (binary): [type 1B][room 4B][sender 4B][PCM...]   type 0 = keepalive, 1 = audio
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => { res.writeHead(200); res.end('voice relay ok'); });
const wss = new WebSocketServer({ server });
const rooms = new Map(); // roomId -> Set<ws>

function leave(ws) {
  if (ws.room === null || ws.room === undefined) return;
  const g = rooms.get(ws.room);
  if (g) { g.delete(ws); if (g.size === 0) rooms.delete(ws.room); }
  ws.room = null;
}

wss.on('connection', (ws) => {
  ws.room = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data, isBinary) => {
    if (!isBinary || !Buffer.isBuffer(data) || data.length < 9) return;
    ws.isAlive = true;
    const room = data.readInt32BE(1);
    if (ws.room !== room) {
      leave(ws);
      ws.room = room;
      let g = rooms.get(room);
      if (!g) { g = new Set(); rooms.set(room, g); }
      g.add(ws);
    }
    if (data[0] === 1) {
      for (const c of rooms.get(room)) {
        if (c !== ws && c.readyState === 1) c.send(data, { binary: true });
      }
    }
  });
  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

// Ping every 25s: keeps idle proxies from closing the connection, drops dead clients.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log('Voice relay listening on ' + PORT));
