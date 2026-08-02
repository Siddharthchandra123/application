const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const USERS_FILE = path.join(DATA_DIR, 'users.json');
const LOGS_FILE = path.join(DATA_DIR, 'audit_logs.json');
const POSTS_FILE = path.join(DATA_DIR, 'posts.json');

// Initialize files if they don't exist
if (!fs.existsSync(USERS_FILE)) fs.writeFileSync(USERS_FILE, JSON.stringify([]));
if (!fs.existsSync(LOGS_FILE)) fs.writeFileSync(LOGS_FILE, JSON.stringify([]));
if (!fs.existsSync(POSTS_FILE)) fs.writeFileSync(POSTS_FILE, JSON.stringify([]));

function readData(filePath) {
  try {
    const data = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    return [];
  }
}

function writeData(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
  } catch (err) {
    console.error('Failed to write data:', err);
  }
}

const db = {
  getUsers: () => readData(USERS_FILE),
  saveUsers: (users) => writeData(USERS_FILE, users),
  getLogs: () => readData(LOGS_FILE),
  saveLogs: (logs) => writeData(LOGS_FILE, logs),
  getPosts: () => readData(POSTS_FILE),
  savePosts: (posts) => writeData(POSTS_FILE, posts),
};

module.exports = {
  // Find user by username
  getUserByUsername: (username) => {
    const users = db.getUsers();
    return users.find(u => u.username === username) || null;
  },
  
  // Find user by ID
  getUserById: (id) => {
    const users = db.getUsers();
    return users.find(u => u.id === id) || null;
  },

  // Create user
  createUser: (userId, username, passwordHash, nickname, callback) => {
    const users = db.getUsers();
    if (users.some(u => u.username === username)) {
      return callback(new Error('UNIQUE constraint failed'));
    }
    const newUser = {
      id: userId,
      username,
      password_hash: passwordHash,
      nickname,
      avatar: null,
      theme: 'aurora',
      created_at: Date.now()
    };
    users.push(newUser);
    db.saveUsers(users);
    callback(null, newUser);
  },

  // Update user profile
  updateUser: (userId, nickname, avatar, passwordHash, theme, callback) => {
    const users = db.getUsers();
    const index = users.findIndex(u => u.id === userId);
    if (index === -1) {
      return callback(new Error('User not found'));
    }
    if (nickname !== undefined) users[index].nickname = nickname;
    if (avatar !== undefined) users[index].avatar = avatar;
    if (passwordHash !== undefined) users[index].password_hash = passwordHash;
    if (theme !== undefined) users[index].theme = theme;
    db.saveUsers(users);
    callback(null, users[index]);
  },

  // Log audit event
  logAuditEvent: (eventType, userId, username, roomId, details, ip) => {
    const logs = db.getLogs();
    const newLog = {
      id: logs.length + 1,
      event_type: eventType,
      user_id: userId,
      username,
      room_id: roomId,
      details,
      ip_address: ip || 'unknown',
      timestamp: Date.now()
    };
    logs.push(newLog);
    db.saveLogs(logs);
  },

  // Get all posts
  getAllPosts: () => {
    return db.getPosts();
  },

  // Create new post
  createPost: (userId, nickname, avatar, content, media, callback) => {
    const posts = db.getPosts();
    const newPost = {
      id: 'post_' + Math.random().toString(36).substring(2, 10),
      user_id: userId,
      nickname,
      avatar,
      content,
      media: media || null,
      likes: [],
      timestamp: Date.now()
    };
    posts.unshift(newPost);
    db.savePosts(posts);
    callback(null, newPost);
  },

  // Toggle like post
  likePost: (postId, userId, callback) => {
    const posts = db.getPosts();
    const post = posts.find(p => p.id === postId);
    if (!post) {
      return callback(new Error('Post not found'));
    }
    if (!post.likes) post.likes = [];
    const likeIndex = post.likes.indexOf(userId);
    if (likeIndex === -1) {
      post.likes.push(userId);
    } else {
      post.likes.splice(likeIndex, 1);
    }
    db.savePosts(posts);
    callback(null, post);
  },

  // Delete post
  deletePost: (postId, userId, callback) => {
    const posts = db.getPosts();
    const index = posts.findIndex(p => p.id === postId);
    if (index === -1) {
      return callback(new Error('Post not found'));
    }
    if (posts[index].user_id !== userId) {
      return callback(new Error('Unauthorized to delete this post'));
    }
    posts.splice(index, 1);
    db.savePosts(posts);
    callback(null, { success: true });
  }
};
