// VoxelCraft Relay + Persistence Server
// =====================================
//
// A lightweight WebSocket server that:
//   1. Relays messages between players in the same room (so multiplayer
//      works even behind strict NATs that block WebRTC).
//   2. Persists world state (modified blocks) per room in memory + on disk,
//      so the world survives even when NO players are online.
//   3. Sends the full world state to any player who joins a room.
//
// Deployment:
//   - Render:  https://render.com  (free tier available)
//   - Railway: https://railway.app (free trial, then ~$5/month)
//   - Fly.io:  https://fly.io      (free tier available)
//
// After deploying, set the SERVER_URL in your HTML file to your deployed URL:
//   const SERVER_URL = 'wss://your-app-name.onrender.com';
//
// =====================================

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// In-memory world state: roomCode -> { blocks: Map<"x,y,z", blockType>, seed, lastModified }
// This is periodically saved to disk so it survives server restarts.
const rooms = new Map();

// Load saved rooms from disk on startup
const DATA_FILE = path.join(__dirname, 'rooms.json');
function loadRooms() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = fs.readFileSync(DATA_FILE, 'utf8');
      const data = JSON.parse(raw);
      for (const [code, room] of Object.entries(data)) {
        const blocks = new Map(room.blocks);
        rooms.set(code, { blocks, seed: room.seed, lastModified: room.lastModified });
      }
      console.log(`[Server] Loaded ${rooms.size} rooms from disk`);
    }
  } catch (e) {
    console.error('[Server] Load error:', e.message);
  }
}

// Save rooms to disk (throttled)
let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    const data = {};
    for (const [code, room] of rooms) {
      data[code] = {
        blocks: Array.from(room.blocks.entries()),
        seed: room.seed,
        lastModified: room.lastModified
      };
    }
    fs.writeFile(DATA_FILE, JSON.stringify(data), (err) => {
      if (err) console.error('[Server] Save error:', err.message);
    });
  }, 5000);
}

// Get or create a room
function getRoom(code) {
  let room = rooms.get(code);
  if (!room) {
    room = { blocks: new Map(), seed: null, lastModified: Date.now() };
    rooms.set(code, room);
  }
  return room;
}

// Track WebSocket connections per room: roomCode -> Set<WebSocket>
const roomClients = new Map();

function getClients(code) {
  let clients = roomClients.get(code);
  if (!clients) {
    clients = new Set();
    roomClients.set(code, clients);
  }
  return clients;
}

function broadcast(code, message, except = null) {
  const clients = getClients(code);
  const data = JSON.stringify(message);
  for (const client of clients) {
    if (client !== except && client.readyState === 1) { // OPEN
      client.send(data);
    }
  }
}

// Create HTTP server (for health checks + serving the game if you want)
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      rooms: rooms.size,
      totalBlocks: Array.from(rooms.values()).reduce((sum, r) => sum + r.blocks.size, 0),
      uptime: process.uptime()
    }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('VoxelCraft server running. Connect via WebSocket.');
});

// Create WebSocket server
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  let currentRoom = null;
  let playerName = 'Player';

  console.log('[Server] New WebSocket connection');

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      return;
    }
    if (!msg.type) return;

    // === JOIN: player joins a room ===
    if (msg.type === 'join') {
      // Leave previous room if any
      if (currentRoom) {
        const clients = getClients(currentRoom);
        clients.delete(ws);
        broadcast(currentRoom, { type: 'leave', name: playerName });
      }

      currentRoom = (msg.roomCode || 'DEFAULT').toUpperCase();
      playerName = msg.name || 'Player';
      ws.playerName = playerName;

      const clients = getClients(currentRoom);
      clients.add(ws);

      console.log(`[Server] ${playerName} joined room ${currentRoom} (${clients.size} players)`);

      // Send the full world state to the joining player
      const room = getRoom(currentRoom);
      if (msg.seed) room.seed = msg.seed;
      const blocks = Array.from(room.blocks.entries()).map(([key, type]) => {
        const parts = key.split(',');
        return [parseInt(parts[0]), parseInt(parts[1]), parseInt(parts[2]), type];
      });

      // Send in chunks of 500 (to stay under message size limits)
      const CHUNK = 500;
      for (let i = 0; i < blocks.length; i += CHUNK) {
        const chunk = blocks.slice(i, i + CHUNK);
        const isLast = i + CHUNK >= blocks.length;
        ws.send(JSON.stringify({
          type: 'world-sync',
          blocks: chunk,
          done: isLast,
          totalBlocks: blocks.length,
          chunkIndex: Math.floor(i / CHUNK),
          totalChunks: Math.ceil(blocks.length / CHUNK)
        }));
      }

      // Notify other players in the room
      broadcast(currentRoom, { type: 'join', name: playerName }, ws);

      // Send list of online players to the new joiner
      const onlinePlayers = [];
      for (const client of clients) {
        if (client !== ws && client.playerName) {
          onlinePlayers.push(client.playerName);
        }
      }
      ws.send(JSON.stringify({ type: 'players', players: onlinePlayers }));
    }

    // === BLOCK: player places/breaks a block ===
    else if (msg.type === 'block' && currentRoom) {
      const room = getRoom(currentRoom);
      const key = msg.x + ',' + msg.y + ',' + msg.z;
      if (msg.blockType === 0) {
        // Air = block removed
        room.blocks.delete(key);
      } else {
        room.blocks.set(key, msg.blockType);
      }
      room.lastModified = Date.now();
      scheduleSave();

      // Relay to all other players in the room
      broadcast(currentRoom, {
        type: 'block',
        x: msg.x, y: msg.y, z: msg.z,
        blockType: msg.blockType,
        oldBlock: msg.oldBlock,
        name: playerName
      }, ws);
    }

    // === MOVE: player position update ===
    else if (msg.type === 'move' && currentRoom) {
      // Relay to other players (don't store — too much data)
      broadcast(currentRoom, {
        type: 'move',
        name: playerName,
        x: msg.x, y: msg.y, z: msg.z, yaw: msg.yaw
      }, ws);
    }

    // === CHAT: chat message ===
    else if (msg.type === 'chat' && currentRoom) {
      broadcast(currentRoom, {
        type: 'chat',
        name: playerName,
        message: msg.message
      }); // include sender so everyone sees it
    }

    // === ATTACK: PvP attack ===
    else if (msg.type === 'attack' && currentRoom) {
      broadcast(currentRoom, {
        type: 'attack',
        target: msg.target,
        name: playerName,
        damage: msg.damage
      });
    }
  });

  ws.on('close', () => {
    if (currentRoom) {
      const clients = getClients(currentRoom);
      clients.delete(ws);
      console.log(`[Server] ${playerName} left room ${currentRoom} (${clients.size} players)`);
      broadcast(currentRoom, { type: 'leave', name: playerName });

      // Clean up empty rooms from roomClients (but keep world state in rooms Map)
      if (clients.size === 0) {
        roomClients.delete(currentRoom);
      }
    }
  });

  ws.on('error', (err) => {
    console.error('[Server] WebSocket error:', err.message);
  });
});

// Save rooms on shutdown
process.on('SIGINT', () => {
  console.log('[Server] Shutting down, saving rooms...');
  const data = {};
  for (const [code, room] of rooms) {
    data[code] = {
      blocks: Array.from(room.blocks.entries()),
      seed: room.seed,
      lastModified: room.lastModified
    };
  }
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(data));
  } catch (e) {
    console.error('[Server] Final save error:', e.message);
  }
  process.exit(0);
});

// Periodic save every 30 seconds
setInterval(scheduleSave, 30000);

// Clean up rooms older than 90 days with no blocks (garbage collection)
setInterval(() => {
  const now = Date.now();
  const NINETY_DAYS = 90 * 24 * 60 * 60 * 1000;
  for (const [code, room] of rooms) {
    if (room.blocks.size === 0 && now - room.lastModified > NINETY_DAYS) {
      rooms.delete(code);
      console.log(`[Server] GC: deleted empty room ${code}`);
    }
  }
}, 3600000); // hourly

loadRooms();

server.listen(PORT, () => {
  console.log(`[Server] VoxelCraft server running on port ${PORT}`);
  console.log(`[Server] Health check: http://localhost:${PORT}/health`);
  console.log(`[Server] WebSocket endpoint: ws://localhost:${PORT}/ws`);
});
