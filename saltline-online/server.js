// Saltline game server: serves the game page and relays players' state over WebSockets.
// Players are grouped into arenas of up to 6. Each player sends their own state; 20 times a
// second the server sends every player in an arena a snapshot of everyone's latest state.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ARENA_SIZE = 6;
const MAX_STATE_BYTES = 4096;

const page = fs.readFileSync(path.join(__dirname, 'index.html'));

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    res.end(page);
  } else if (url === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.end('ok');
  } else {
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('Not found');
  }
});

const wss = new WebSocketServer({ server, maxPayload: MAX_STATE_BYTES * 2 });
const arenas = new Map(); // name -> Set of sockets
let nextId = 1;
let arenaCounter = 0;

function pickArena() {
  for (const [name, players] of arenas) if (players.size < ARENA_SIZE) return name;
  const name = 'arena-' + (++arenaCounter);
  arenas.set(name, new Set());
  return name;
}

wss.on('connection', ws => {
  ws.id = (nextId++).toString(36) + Math.random().toString(36).slice(2, 6);
  ws.arena = pickArena();
  ws.state = null;
  ws.alive = true;
  arenas.get(ws.arena).add(ws);
  ws.send(JSON.stringify({ t: 'hello', id: ws.id, arena: ws.arena }));

  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', data => {
    if (data.length > MAX_STATE_BYTES * 2) return;
    let msg;
    try { msg = JSON.parse(data); } catch (e) { return; }
    if (msg && msg.t === 'p' && msg.pr && typeof msg.pr === 'object' && !Array.isArray(msg.pr)) {
      if (JSON.stringify(msg.pr).length <= MAX_STATE_BYTES) ws.state = msg.pr;
    }
  });
  ws.on('close', () => {
    const players = arenas.get(ws.arena);
    if (!players) return;
    players.delete(ws);
    if (players.size === 0) arenas.delete(ws.arena);
  });
});

// 20 snapshots a second to every arena
setInterval(() => {
  for (const players of arenas.values()) {
    const peers = [];
    for (const p of players) if (p.state) peers.push({ id: p.id, pr: p.state });
    const msg = JSON.stringify({ t: 'snap', peers });
    for (const p of players) if (p.readyState === 1) p.send(msg);
  }
}, 50);

// drop connections that stopped answering
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log('Saltline is running on port ' + PORT));
