require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const { AccessToken } = require('livekit-server-sdk');
const { Kafka } = require('kafkajs');
const cookieParser = require('cookie-parser');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const path = require('path');

const app = express();
app.use(cors({
  origin: true,
  credentials: true
}));
app.use(express.json());
app.use(cookieParser());

const JWT_SECRET = process.env.JWT_SECRET || 'friendverse_super_secret_fallback_key';

// Import file-based database helper (pure JS fallback for Render compatibility)
const db = require('./db');
const logAuditEvent = db.logAuditEvent;

// Initialize Kafka client if broker is configured
let kafka, producer, consumer;
const KAFKA_BROKER = process.env.KAFKA_BROKER;

const localHistory = new Map(); // roomId -> { chat, memories, timeline }

function getLocalRoomState(roomId) {
  let state = localHistory.get(roomId);
  if (!state) {
    state = {
      chat: [],
      memories: [],
      timeline: [
        { id: '1', year: '2020', text: 'We met for the first time!' },
        { id: '2', year: '2022', text: 'Our legendary road trip 🚗' },
        { id: '3', year: '2024', text: 'Graduation Day! 🎓' },
        { id: '4', year: '2026', text: 'Still best friends forever! ❤️' }
      ]
    };
    localHistory.set(roomId, state);
  }
  return state;
}

// Background Kafka connection and consumer runner
async function startKafka() {
  const isRender = process.env.RENDER === 'true' || process.env.NODE_ENV === 'production';
  const finalBroker = (isRender && KAFKA_BROKER === 'localhost:9092') ? null : KAFKA_BROKER;

  if (!finalBroker) {
    console.log('Running in cloud/fallback mode (no local Kafka broker connection attempted). Using local memory queues.');
    return;
  }

  try {
    console.log(`Connecting to self-hosted Kafka broker at ${finalBroker}...`);
    const { logLevel } = require('kafkajs');
    kafka = new Kafka({
      clientId: 'friendverse-gateway',
      brokers: [finalBroker],
      logLevel: logLevel.ERROR, // Suppress verbose warning/info logs
      retry: {
        initialRetryTime: 100,
        retries: 1 // Fail fast if broker is unavailable
      }
    });

    producer = kafka.producer();
    // Unique group ID per gateway node so they all get a copy of room events (Pub/Sub pattern)
    consumer = kafka.consumer({ 
      groupId: 'friendverse-group-' + Math.random().toString(36).substring(2, 8) 
    });

    const admin = kafka.admin();
    await admin.connect();
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes('friendverse-events')) {
      console.log('Creating topic "friendverse-events"...');
      await admin.createTopics({
        topics: [{
          topic: 'friendverse-events',
          numPartitions: 1,
          replicationFactor: 1
        }]
      });
    }
    await admin.disconnect();

    await producer.connect();
    await consumer.connect();
    
    // Subscribe to friendverse events
    await consumer.subscribe({ topic: 'friendverse-events', fromBeginning: true });
    console.log('Kafka Producer and Consumer connected successfully.');

    // Event Sourcing replay / updates loop
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          if (!message.value) return;
          const event = JSON.parse(message.value.toString());
          const { roomId, type, payload, senderId } = event;

          console.log(`Kafka Event received: ${type} for room ${roomId}`);

          // Sync local in-memory history cache
          const localState = getLocalRoomState(roomId);
          
          if (type === 'chat') {
            if (!localState.chat.some(c => c.id === payload.id)) {
              localState.chat.push(payload);
            }
            // Relay chat to other users in the room
            broadcastToRoom(roomId, null, 'chat', { senderId, message: payload });
          } else if (type === 'memory-add') {
            if (!localState.memories.some(m => m.id === payload.id)) {
              localState.memories.push(payload);
            }
            broadcastToRoom(roomId, null, 'memory-add', { item: payload });
          } else if (type === 'memory-delete') {
            localState.memories = localState.memories.filter(m => m.id !== payload.id);
            broadcastToRoom(roomId, null, 'memory-delete', { id: payload.id });
          } else if (type === 'timeline-add') {
            if (!localState.timeline.some(t => t.id === payload.id)) {
              localState.timeline.push(payload);
            }
            broadcastToRoom(roomId, null, 'timeline-add', { event: payload });
          } else if (type === 'timeline-delete') {
            localState.timeline = localState.timeline.filter(t => t.id !== payload.id);
            broadcastToRoom(roomId, null, 'timeline-delete', { id: payload.id });
          }
        } catch (e) {
          console.error('Error parsing or handling consumed message:', e);
        }
      }
    });
  } catch (err) {
    console.error('Failed to connect to self-hosted Kafka. Falling back to local events:', err);
    producer = null;
    consumer = null;
  }
}

// Trigger background Kafka start
startKafka();

async function publishEvent(roomId, eventType, payload, senderId) {
  // Update local memory state (fallback / caching)
  const localState = getLocalRoomState(roomId);
  if (!producer) {
    if (eventType === 'chat') {
      localState.chat.push(payload);
    } else if (eventType === 'memory-add') {
      localState.memories.push(payload);
    } else if (eventType === 'memory-delete') {
      localState.memories = localState.memories.filter(m => m.id !== payload.id);
    } else if (eventType === 'timeline-add') {
      localState.timeline.push(payload);
    } else if (eventType === 'timeline-delete') {
      localState.timeline = localState.timeline.filter(t => t.id !== payload.id);
    }
  }

  // Publish event to Kafka cluster if available
  if (producer) {
    try {
      const event = {
        roomId,
        type: eventType,
        payload,
        senderId,
        timestamp: Date.now()
      };
      await producer.send({
        topic: 'friendverse-events',
        messages: [{
          key: roomId,
          value: JSON.stringify(event)
        }]
      });
      console.log(`Published event ${eventType} to Kafka for room ${roomId}`);
    } catch (err) {
      console.error(`Error sending message to Kafka:`, err);
    }
  }
}

// Authentication Routes

app.post('/api/auth/register', async (req, res) => {
  const { username, nickname, password } = req.body;
  if (!username || !nickname || !password) {
    return res.status(400).json({ error: 'All fields are required' });
  }

  const cleanUsername = username.trim().toLowerCase();
  
  try {
    const salt = await bcrypt.genSalt(10);
    const passwordHash = await bcrypt.hash(password, salt);
    const userId = 'usr_' + Math.random().toString(36).substring(2, 10);

    db.createUser(userId, cleanUsername, passwordHash, nickname.trim(), (err, newUser) => {
      if (err) {
        if (err.message.includes('UNIQUE constraint failed')) {
          return res.status(400).json({ error: 'Username is already taken' });
        }
        return res.status(500).json({ error: 'Database operation failed' });
      }

      const token = jwt.sign({ userId }, JWT_SECRET, { expiresIn: '7d' });
      
      res.cookie('token', token, {
        httpOnly: true,
        secure: process.env.NODE_ENV === 'production',
        sameSite: process.env.NODE_ENV === 'production' ? 'none' : 'lax',
        maxAge: 7 * 24 * 60 * 60 * 1000 // 7 days
      });

      logAuditEvent('USER_REGISTER', userId, cleanUsername, null, { nickname: nickname.trim() }, req.ip);

      res.status(201).json({
        user: { id: userId, username: cleanUsername, nickname: nickname.trim(), avatar: null, theme: 'aurora' }
      });
    });
  } catch (err) {
    console.error('Registration error:', err);
    res.status(500).json({ error: 'Registration failed' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: 'All fields are required' });
  }

  const cleanUsername = username.trim().toLowerCase();
  const user = db.getUserByUsername(cleanUsername);
  
  if (!user) {
    return res.status(400).json({ error: 'Invalid credentials' });
  }

  try {
    const isMatch = await bcrypt.compare(password, user.password_hash);
    if (!isMatch) {
      return res.status(400).json({ error: 'Invalid credentials' });
    }

    const token = jwt.sign({ userId: user.id }, JWT_SECRET, { expiresIn: '7d' });
    
    res.cookie('token', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: process.env.NODE_ENV === 'production' ? 'none' : 'lax',
      maxAge: 7 * 24 * 60 * 60 * 1000 // 7 days
    });

    logAuditEvent('USER_LOGIN', user.id, user.username, null, null, req.ip);

    res.json({
      user: { id: user.id, username: user.username, nickname: user.nickname, avatar: user.avatar, theme: user.theme || 'aurora' }
    });
  } catch (e) {
    res.status(500).json({ error: 'Login failed' });
  }
});

app.get('/api/auth/me', (req, res) => {
  const token = req.cookies.token;
  if (!token) {
    return res.status(401).json({ error: 'Not authenticated' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const user = db.getUserById(decoded.userId);
    if (!user) {
      return res.status(401).json({ error: 'User not found' });
    }
    res.json({
      user: { id: user.id, username: user.username, nickname: user.nickname, avatar: user.avatar, theme: user.theme || 'aurora' }
    });
  } catch (err) {
    res.status(401).json({ error: 'Invalid token' });
  }
});

app.post('/api/auth/logout', (req, res) => {
  const token = req.cookies.token;
  if (token) {
    try {
      const decoded = jwt.verify(token, JWT_SECRET);
      logAuditEvent('USER_LOGOUT', decoded.userId, null, null, null, req.ip);
    } catch (err) {}
  }
  res.clearCookie('token', {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: process.env.NODE_ENV === 'production' ? 'none' : 'lax'
  });
  res.json({ success: true });
});

app.post('/api/auth/profile', async (req, res) => {
  const token = req.cookies.token;
  if (!token) {
    return res.status(401).json({ error: 'Not authenticated' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const { nickname, avatar, password, theme } = req.body;
    const user = db.getUserById(decoded.userId);

    if (!user) {
      return res.status(401).json({ error: 'User not found' });
    }

    let passwordHash = user.password_hash;
    if (password && password.trim().length > 0) {
      const salt = await bcrypt.genSalt(10);
      passwordHash = await bcrypt.hash(password, salt);
    }

    const updatedNickname = nickname !== undefined ? nickname.trim() : user.nickname;
    const updatedAvatar = avatar !== undefined ? avatar : user.avatar;
    const updatedTheme = theme !== undefined ? theme : (user.theme || 'aurora');

    db.updateUser(decoded.userId, updatedNickname, updatedAvatar, passwordHash, updatedTheme, (err, updatedUser) => {
      if (err) {
        return res.status(500).json({ error: 'Failed to update profile' });
      }
      logAuditEvent('USER_PROFILE_UPDATE', user.id, user.username, null, { changedNickname: nickname !== user.nickname, changedAvatar: !!avatar, changedPassword: !!password, changedTheme: theme !== user.theme }, req.ip);
      res.json({
        user: { id: user.id, username: user.username, nickname: updatedNickname, avatar: updatedAvatar, theme: updatedTheme }
      });
    });
  } catch (err) {
    res.status(401).json({ error: 'Invalid token' });
  }
});

// Posts REST API Endpoints

app.get('/api/posts', (req, res) => {
  const posts = db.getAllPosts();
  res.json({ posts });
});

app.post('/api/posts', (req, res) => {
  const token = req.cookies.token;
  if (!token) {
    return res.status(401).json({ error: 'Not authenticated' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const user = db.getUserById(decoded.userId);
    if (!user) {
      return res.status(401).json({ error: 'User not found' });
    }

    const { content, media } = req.body;
    if (!content && !media) {
      return res.status(400).json({ error: 'Content or media is required' });
    }

    db.createPost(user.id, user.nickname, user.avatar, content, media, (err, newPost) => {
      if (err) {
        return res.status(500).json({ error: 'Failed to create post' });
      }
      logAuditEvent('POST_CREATE', user.id, user.username, null, { postId: newPost.id }, req.ip);
      res.status(201).json({ post: newPost });
    });
  } catch (err) {
    res.status(401).json({ error: 'Invalid token' });
  }
});

app.post('/api/posts/:id/like', (req, res) => {
  const token = req.cookies.token;
  if (!token) {
    return res.status(401).json({ error: 'Not authenticated' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const user = db.getUserById(decoded.userId);
    if (!user) {
      return res.status(401).json({ error: 'User not found' });
    }

    const postId = req.params.id;
    db.likePost(postId, user.id, (err, updatedPost) => {
      if (err) {
        return res.status(400).json({ error: err.message });
      }
      res.json({ post: updatedPost });
    });
  } catch (err) {
    res.status(401).json({ error: 'Invalid token' });
  }
});

app.delete('/api/posts/:id', (req, res) => {
  const token = req.cookies.token;
  if (!token) {
    return res.status(401).json({ error: 'Not authenticated' });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const user = db.getUserById(decoded.userId);
    if (!user) {
      return res.status(401).json({ error: 'User not found' });
    }

    const postId = req.params.id;
    db.deletePost(postId, user.id, (err) => {
      if (err) {
        return res.status(400).json({ error: err.message });
      }
      logAuditEvent('POST_DELETE', user.id, user.username, null, { postId }, req.ip);
      res.json({ success: true });
    });
  } catch (err) {
    res.status(401).json({ error: 'Invalid token' });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.send({ status: 'ok', timestamp: new Date() });
});

// Endpoint to generate LiveKit Access Tokens
app.get('/api/livekit-token', async (req, res) => {
  const { roomId, nickname, socketId } = req.query;
  if (!roomId || !nickname) {
    return res.status(400).json({ error: 'roomId and nickname are required' });
  }

  const apiKey = process.env.LIVEKIT_API_KEY;
  const apiSecret = process.env.LIVEKIT_API_SECRET;
  const livekitUrl = process.env.LIVEKIT_URL;

  if (!apiKey || !apiSecret || !livekitUrl) {
    console.warn('LiveKit credentials missing, returning mock connection details.');
    return res.json({ token: 'mock-token', serverUrl: 'ws://localhost:7880', isMock: true });
  }

  try {
    const identity = socketId || (nickname + '_' + Math.random().toString(36).substring(2, 6));
    const at = new AccessToken(apiKey, apiSecret, {
      identity: identity,
      name: nickname,
    });

    at.addGrant({
      roomJoin: true,
      room: roomId,
      canPublish: true,
      canSubscribe: true,
    });

    const token = await at.toJwt();
    res.json({ token, serverUrl: livekitUrl });
  } catch (error) {
    console.error('Error generating LiveKit token:', error);
    res.status(500).json({ error: 'Failed to generate token' });
  }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Active rooms: roomId -> Set of WebSocket instances
const activeRooms = new Map();

// Mapping of WebSocket instance -> { id: string, nickname: string, roomId: string }
const wsClients = new Map();

wss.on('connection', (ws, req) => {
  const getSocketIp = (request) => {
    return request.headers['x-forwarded-for'] || request.socket.remoteAddress;
  };
  const socketId = 'sock_' + Math.random().toString(36).substring(2, 10);
  wsClients.set(ws, { id: socketId, nickname: 'anonymous', roomId: null });

  console.log(`User connected: ${socketId}`);

  ws.on('message', async (message) => {
    try {
      const { event, roomId, ...payload } = JSON.parse(message.toString());
      const clientInfo = wsClients.get(ws);
      if (!clientInfo) return;

      if (event === 'join-room') {
        const { nickname } = payload;
        clientInfo.nickname = nickname || 'anonymous';
        console.log(`User ${socketId} (${clientInfo.nickname}) requesting to join room: ${roomId}`);

        if (!roomId) {
          ws.send(JSON.stringify({ event: 'error-msg', message: 'Invalid Room ID' }));
          return;
        }

        let clientsSet = activeRooms.get(roomId);
        if (!clientsSet) {
          clientsSet = new Set();
          activeRooms.set(roomId, clientsSet);
        }

        if (clientsSet.size >= 5) {
          console.log(`Room ${roomId} is full. User ${socketId} rejected.`);
          ws.send(JSON.stringify({ event: 'room-full' }));
          return;
        }

        // Join room
        clientInfo.roomId = roomId;
        clientsSet.add(ws);

        console.log(`User ${socketId} successfully joined room ${roomId}. Room size: ${clientsSet.size}`);
        logAuditEvent('ROOM_JOIN', null, clientInfo.nickname, roomId, { socketId }, getSocketIp(req));

        // Get other users
        const otherUsers = [];
        clientsSet.forEach(cWs => {
          if (cWs !== ws) {
            const info = wsClients.get(cWs);
            if (info) {
              otherUsers.push({ id: info.id, nickname: info.nickname });
            }
          }
        });

        const history = getLocalRoomState(roomId);
        ws.send(JSON.stringify({ event: 'joined', roomId, otherUsers, history }));

        // Notify existing peers
        broadcastToRoom(roomId, ws, 'peer-joined', { peerId: socketId, nickname: clientInfo.nickname });
      }

      else if (event === 'chat') {
        const { message: chatMsg } = payload;
        logAuditEvent('ROOM_CHAT', null, clientInfo.nickname, roomId, { message: chatMsg }, getSocketIp(req));
        if (producer) {
          await publishEvent(roomId, 'chat', chatMsg, socketId);
        } else {
          const localState = getLocalRoomState(roomId);
          localState.chat.push(chatMsg);
          broadcastToRoom(roomId, ws, 'chat', { senderId: socketId, message: chatMsg });
        }
      }

      else if (event === 'typing') {
        const { isTyping } = payload;
        broadcastToRoom(roomId, ws, 'typing', { senderId: socketId, isTyping });
      }

      else if (event === 'reaction') {
        const { emoji } = payload;
        broadcastToRoom(roomId, ws, 'reaction', { emoji });
      }

      else if (event === 'draw-stroke') {
        const { stroke } = payload;
        broadcastToRoom(roomId, ws, 'draw-stroke', { stroke });
      }

      else if (event === 'draw-clear') {
        broadcastToRoom(roomId, ws, 'draw-clear', {});
      }

      else if (event === 'draw-undo') {
        const { remainingStrokes } = payload;
        broadcastToRoom(roomId, ws, 'draw-undo', { remainingStrokes });
      }

      else if (event === 'memory-add') {
        const { item } = payload;
        logAuditEvent('ROOM_MEMORY_ADD', null, clientInfo.nickname, roomId, { item }, getSocketIp(req));
        if (producer) {
          await publishEvent(roomId, 'memory-add', item, socketId);
        } else {
          const localState = getLocalRoomState(roomId);
          localState.memories.push(item);
          broadcastToRoom(roomId, ws, 'memory-add', { item });
        }
      }

      else if (event === 'memory-delete') {
        const { id } = payload;
        logAuditEvent('ROOM_MEMORY_DELETE', null, clientInfo.nickname, roomId, { id }, getSocketIp(req));
        if (producer) {
          await publishEvent(roomId, 'memory-delete', { id }, socketId);
        } else {
          const localState = getLocalRoomState(roomId);
          localState.memories = localState.memories.filter(m => m.id !== id);
          broadcastToRoom(roomId, ws, 'memory-delete', { id });
        }
      }

      else if (event === 'timeline-add') {
        const { event: ev } = payload;
        logAuditEvent('ROOM_TIMELINE_ADD', null, clientInfo.nickname, roomId, { event: ev }, getSocketIp(req));
        if (producer) {
          await publishEvent(roomId, 'timeline-add', ev, socketId);
        } else {
          const localState = getLocalRoomState(roomId);
          localState.timeline.push(ev);
          broadcastToRoom(roomId, ws, 'timeline-add', { event: ev });
        }
      }

      else if (event === 'timeline-delete') {
        const { id } = payload;
        logAuditEvent('ROOM_TIMELINE_DELETE', null, clientInfo.nickname, roomId, { id }, getSocketIp(req));
        if (producer) {
          await publishEvent(roomId, 'timeline-delete', { id }, socketId);
        } else {
          const localState = getLocalRoomState(roomId);
          localState.timeline = localState.timeline.filter(t => t.id !== id);
          broadcastToRoom(roomId, ws, 'timeline-delete', { id });
        }
      }

      else if (event === 'select-game') {
        const { game } = payload;
        broadcastToRoom(roomId, ws, 'select-game', { game });
      }

      else if (event === 'game-action') {
        broadcastToRoom(roomId, ws, 'game-action', payload);
      }

      else if (event === 'quiz-action') {
        broadcastToRoom(roomId, ws, 'quiz-action', payload);
      }

      else if (event === 'quiz-reset') {
        broadcastToRoom(roomId, ws, 'quiz-reset', {});
      }

      else if (event === 'meter-action') {
        broadcastToRoom(roomId, ws, 'meter-action', payload);
      }

      else if (event === 'meter-reset') {
        broadcastToRoom(roomId, ws, 'meter-reset', {});
      }

      else if (event === 'surprise') {
        const { surpriseType, message: surpriseMsg } = payload;
        broadcastToRoom(roomId, ws, 'surprise', { surpriseType, message: surpriseMsg });
      }

      else if (event === 'leave-room') {
        console.log(`User ${socketId} leaving room ${roomId}`);
        handleUserLeave(ws);
      }

    } catch (e) {
      console.error('Error handling WebSocket message:', e);
    }
  });

  ws.on('close', () => {
    console.log(`User disconnected: ${socketId}`);
    handleUserLeave(ws);
    wsClients.delete(ws);
  });
});

const broadcastToRoom = (roomId, senderWs, event, payload) => {
  const clientsSet = activeRooms.get(roomId);
  if (!clientsSet) return;
  const msg = JSON.stringify({ event, ...payload });
  clientsSet.forEach((cWs) => {
    if (cWs !== senderWs && cWs.readyState === 1) { // 1 = OPEN
      cWs.send(msg);
    }
  });
};

function handleUserLeave(ws) {
  const clientInfo = wsClients.get(ws);
  if (!clientInfo || !clientInfo.roomId) return;

  const roomId = clientInfo.roomId;
  const clientsSet = activeRooms.get(roomId);
  if (clientsSet) {
    clientsSet.delete(ws);
    console.log(`User ${clientInfo.id} left room ${roomId}. Remaining size: ${clientsSet.size}`);
    
    if (clientsSet.size === 0) {
      activeRooms.delete(roomId);
      console.log(`Room ${roomId} is empty and deleted.`);
    } else {
      broadcastToRoom(roomId, ws, 'peer-left', { peerId: clientInfo.id });
    }
  }
  clientInfo.roomId = null;
}

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Production server listening on port ${PORT}`);
});
