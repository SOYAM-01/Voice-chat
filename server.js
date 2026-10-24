// WebSocket relay for group voice chat with member list. Works on free hosts (Render, Koyeb, etc).
// Packet (binary): [type 1B][room 4B][sender 4B][payload...]
//   type 0 = keepalive, 1 = audio (raw PCM), 4 = audio (Opus), 2 = hello (payload = UTF-8 name),
//   type 3 = member list from server (payload = JSON [{"id":123,"n":"Name"},...])
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => { res.writeHead(200); res.end('voice relay ok'); });
const wss = new WebSocketServer({ server });
const rooms = new Map(); // roomId -> Set<ws>

function broadcastList(room) {
  const g = rooms.get(room);
  if (!g) return;
  const list = [];
  for (const c of g) list.push({ id: c.uid, n: c.name || '?' });
  const head = Buffer.alloc(9);
  head[0] = 3;
  head.writeInt32BE(room, 1);
  head.writeInt32BE(0, 5);
  const msg = Buffer.concat([head, Buffer.from(JSON.stringify(list), 'utf8')]);
  for (const c of g) if (c.readyState === 1) c.send(msg, { binary: true });
}

function leave(ws) {
  if (ws.room === null || ws.room === undefined) return;
  const room = ws.room;
  const g = rooms.get(room);
  ws.room = null;
  if (g) {
    g.delete(ws);
    if (g.size === 0) rooms.delete(room);
    else broadcastList(room);
  }
}

wss.on('connection', (ws) => {
  ws.room = null;
  ws.uid = 0;
  ws.name = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data, isBinary) => {
    if (!isBinary || !Buffer.isBuffer(data) || data.length < 9) return;
    ws.isAlive = true;
    const type = data[0];
    const room = data.readInt32BE(1);
    ws.uid = data.readInt32BE(5);
    if (ws.room !== room) {
      leave(ws);
      ws.room = room;
      let g = rooms.get(room);
      if (!g) { g = new Set(); rooms.set(room, g); }
      g.add(ws);
    }
    if (type === 2) {
      const name = data.slice(9, 9 + 80).toString('utf8')
        .replace(/[\u0000-\u001f]/g, '').trim().slice(0, 20) || 'User';
      if (ws.name !== name) {
        ws.name = name;
        broadcastList(room);
      }
    } else if (type === 1 || type === 4) {   // 1 = raw PCM, 4 = Opus: relay unchanged to everyone else
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
