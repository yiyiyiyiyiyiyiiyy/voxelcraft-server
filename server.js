// VoxelCraft Relay + Persistence Server (with player state persistence)
// =================================================================
//
// Stores per-room:
//   1. World state (modified blocks) — in `rooms.json`
//   2. Player state (position, inventory, health, etc.) — in `players.json`
//
// When a player joins with a name that has played in this room before,
// they get their previous position + inventory back.
//
// =================================================================

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// rooms: roomCode -> { blocks: Map<"x,y,z", blockType>, seed, lastModified }
// players: roomCode -> Map<playerName, { position, inventory, health, hunger, gamemode, ... }>
const rooms = new Map();
const players = new Map();

const DATA_FILE = path.join(__dirname, 'rooms.json');
const PLAYERS_FILE = path.join(__dirname, 'players.json');

function loadRooms() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = fs.readFileSync(DATA_FILE, 'utf8');
      const data = JSON.parse(raw);
      for (const [code, room] of Object.entries(data)) {
        rooms.set(code, {
          blocks: new Map(room.blocks),
          seed: room.seed,
          lastModified: room.lastModified
        });
      }
      console.log(`[Server] Loaded ${rooms.size} rooms from disk`);
    }
  } catch (e) {
    console.error('[Server] Load rooms error:', e.message);
  }
}

function loadPlayers() {
  try {
    if (fs.existsSync(PLAYERS_FILE)) {
      const raw = fs.readFileSync(PLAYERS_FILE, 'utf8');
      const data = JSON.parse(raw);
      for (const [code, entries] of Object.entries(data)) {
        const playerMap = new Map();
        for (const [pname, pdata] of Object.entries(entries)) {
          playerMap.set(pname, pdata);
        }
        players.set(code, playerMap);
      }
      console.log(`[Server] Loaded player state for ${players.size} rooms from disk`);
    }
  } catch (e) {
    console.error('[Server] Load players error:', e.message);
  }
}

let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveAll();
  }, 5000);
}

function saveAll() {
  try {
    const roomsData = {};
    for (const [code, room] of rooms) {
      roomsData[code] = {
        blocks: Array.from(room.blocks.entries()),
        seed: room.seed,
        lastModified: room.lastModified
      };
    }
    fs.writeFileSync(DATA_FILE, JSON.stringify(roomsData));
  } catch (e) {
    console.error('[Server] Save rooms error:', e.message);
  }
  try {
    const playersData = {};
    for (const [code, playerMap] of players) {
      playersData[code] = Object.fromEntries(playerMap.entries());
    }
    fs.writeFileSync(PLAYERS_FILE, JSON.stringify(playersData));
  } catch (e) {
    console.error('[Server] Save players error:', e.message);
  }
}

function getRoom(code) {
  let room = rooms.get(code);
  if (!room) {
    room = { blocks: new Map(), seed: null, lastModified: Date.now() };
    rooms.set(code, room);
  }
  return room;
}

function getPlayers(code) {
  let playerMap = players.get(code);
  if (!playerMap) {
    playerMap = new Map();
    players.set(code, playerMap);
  }
  return playerMap;
}

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
    if (client !== except && client.readyState === 1) {
      client.send(data);
    }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/keepalive') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      rooms: rooms.size,
      totalBlocks: Array.from(rooms.values()).reduce((sum, r) => sum + r.blocks.size, 0),
      totalPlayers: Array.from(players.values()).reduce((sum, p) => sum + p.size, 0),
      uptime: process.uptime()
    }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('VoxelCraft server running. Connect via WebSocket.');
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  let currentRoom = null;
  let playerName = 'Player';

  console.log('[Server] New WebSocket connection');

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg.type) return;

    // === JOIN ===
    if (msg.type === 'join') {
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

      const room = getRoom(currentRoom);
      if (msg.seed) room.seed = msg.seed;

      // 1. Send world state (all modified blocks) in chunks of 500
      const blocks = Array.from(room.blocks.entries()).map(([key, type]) => {
        const parts = key.split(',');
        return [parseInt(parts[0]), parseInt(parts[1]), parseInt(parts[2]), type];
      });
      const CHUNK = 500;
      const hasSavedPlayer = getPlayers(currentRoom).has(playerName);
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
      // If no blocks at all, send an empty done message
      if (blocks.length === 0) {
        ws.send(JSON.stringify({
          type: 'world-sync',
          blocks: [],
          done: true,
          totalBlocks: 0
        }));
      }

      // 2. Send the player's saved state if it exists (position + inventory)
      const playerMap = getPlayers(currentRoom);
      const savedPlayer = playerMap.get(playerName);
      if (savedPlayer) {
        ws.send(JSON.stringify({
          type: 'player-state',
          state: savedPlayer,
          welcome: 'Welcome back! Your progress has been restored.'
        }));
        console.log(`[Server] Restored state for ${playerName} in room ${currentRoom}`);
      }

      // 3. Notify other players in the room
      broadcast(currentRoom, { type: 'join', name: playerName }, ws);

      // 4. Send list of online players to the new joiner
      const onlinePlayers = [];
      for (const client of clients) {
        if (client !== ws && client.playerName) {
          onlinePlayers.push(client.playerName);
        }
      }
      ws.send(JSON.stringify({ type: 'players', players: onlinePlayers }));
    }

    // === BLOCK ===
    else if (msg.type === 'block' && currentRoom) {
      const room = getRoom(currentRoom);
      const key = msg.x + ',' + msg.y + ',' + msg.z;
      if (msg.blockType === 0) {
        room.blocks.delete(key);
      } else {
        room.blocks.set(key, msg.blockType);
      }
      room.lastModified = Date.now();
      scheduleSave();

      broadcast(currentRoom, {
        type: 'block',
        x: msg.x, y: msg.y, z: msg.z,
        blockType: msg.blockType,
        oldBlock: msg.oldBlock,
        name: playerName
      }, ws);
    }

    // === MOVE ===
    else if (msg.type === 'move' && currentRoom) {
      broadcast(currentRoom, {
        type: 'move',
        name: playerName,
        x: msg.x, y: msg.y, z: msg.z, yaw: msg.yaw
      }, ws);
    }

    // === PLAYER STATE (client periodically sends its state for persistence) ===
    else if (msg.type === 'player-state' && currentRoom) {
      const playerMap = getPlayers(currentRoom);
      playerMap.set(playerName, {
        position: msg.state.position,
        inventory: msg.state.inventory,
        health: msg.state.health,
        hunger: msg.state.hunger,
        air: msg.state.air,
        gamemode: msg.state.gamemode,
        selectedSlot: msg.state.selectedSlot,
        lastSeen: Date.now()
      });
      scheduleSave();
    }

    // === CHAT ===
    else if (msg.type === 'chat' && currentRoom) {
      broadcast(currentRoom, {
        type: 'chat',
        name: playerName,
        message: msg.message
      });
    }

    // === ATTACK ===
    else if (msg.type === 'attack' && currentRoom) {
      broadcast(currentRoom, {
        type: 'attack',
        target: msg.target,
        name: playerName,
        damage: msg.damage
      });
    }

    // === LEAVE (client sends its final state before disconnecting) ===
    else if (msg.type === 'leave' && currentRoom) {
      if (msg.state) {
        const playerMap = getPlayers(currentRoom);
        playerMap.set(playerName, {
          position: msg.state.position,
          inventory: msg.state.inventory,
          health: msg.state.health,
          hunger: msg.state.hunger,
          air: msg.state.air,
          gamemode: msg.state.gamemode,
          selectedSlot: msg.state.selectedSlot,
          lastSeen: Date.now()
        });
        scheduleSave();
      }
    }
  });

  ws.on('close', () => {
    if (currentRoom) {
      const clients = getClients(currentRoom);
      clients.delete(ws);
      console.log(`[Server] ${playerName} left room ${currentRoom} (${clients.size} players)`);
      broadcast(currentRoom, { type: 'leave', name: playerName });
      if (clients.size === 0) {
        roomClients.delete(currentRoom);
      }
    }
  });

  ws.on('error', (err) => {
    console.error('[Server] WebSocket error:', err.message);
  });
});

process.on('SIGINT', () => {
  console.log('[Server] Shutting down, saving state...');
  saveAll();
  process.exit(0);
});

setInterval(scheduleSave, 30000);

// Garbage collection: delete rooms/players older than 90 days
setInterval(() => {
  const now = Date.now();
  const NINETY_DAYS = 90 * 24 * 60 * 60 * 1000;
  for (const [code, room] of rooms) {
    if (room.blocks.size === 0 && now - room.lastModified > NINETY_DAYS) {
      rooms.delete(code);
      console.log(`[Server] GC: deleted empty room ${code}`);
    }
  }
  for (const [code, playerMap] of players) {
    for (const [pname, pdata] of playerMap) {
      if (now - (pdata.lastSeen || 0) > NINETY_DAYS) {
        playerMap.delete(pname);
        console.log(`[Server] GC: deleted inactive player ${pname} from room ${code}`);
      }
    }
    if (playerMap.size === 0) players.delete(code);
  }
}, 3600000);

loadRooms();
loadPlayers();

server.listen(PORT, () => {
  console.log(`[Server] VoxelCraft server running on port ${PORT}`);
  console.log(`[Server] Health: http://localhost:${PORT}/health`);
  console.log(`[Server] WebSocket: ws://localhost:${PORT}/ws`);
});
