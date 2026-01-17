const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

app.use(cors());
app.use(express.json());

/* ================= IN-MEMORY STORAGE ================= */
let tournaments = [];
let matchQueue = [];           // waiting players
let activeMatches = {};        // matchId → match object

/* ================= ROUTES ================= */
app.get("/", (req, res) => {
  res.send("Socket server running");
});

/* ================= SOCKET.IO ================= */
io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  /* -------- TOURNAMENTS -------- */
  socket.on("createTournament", (tournament) => {
    tournaments.push(tournament);
    io.emit("tournamentsUpdate", tournaments);
  });

  socket.on("deleteTournament", (tournamentId) => {
    tournaments = tournaments.filter(t => t.id !== tournamentId);
    io.emit("tournamentsUpdate", tournaments);
  });

  socket.on("registerTournament", ({ tournamentId, username }) => {
    const t = tournaments.find(t => t.id === tournamentId);
    if (t && !t.participants.includes(username) && t.participants.length < t.maxParticipants) {
      t.participants.push(username);
      io.emit("tournamentsUpdate", tournaments);
    }
  });

  socket.on("unregisterTournament", ({ tournamentId, username }) => {
    const t = tournaments.find(t => t.id === tournamentId);
    if (t) {
      t.participants = t.participants.filter(p => p !== username);
      io.emit("tournamentsUpdate", tournaments);
    }
  });

  /* -------- MATCHMAKING -------- */
  socket.on("joinQueue", ({ username, gameType }) => {
    socket.username = username;
    socket.gameType = gameType;

    matchQueue.push({ socketId: socket.id, username, gameType });

    const sameType = matchQueue.filter(p => p.gameType === gameType);
    if (sameType.length >= 2) {
      const p1 = sameType[0];
      const p2 = sameType[1];

      const matchId = "match_" + Date.now();

      activeMatches[matchId] = {
        matchId,
        gameType,
        players: {
          [p1.socketId]: p1.username,
          [p2.socketId]: p2.username
        },
        scores: {
          [p1.socketId]: 0,
          [p2.socketId]: 0
        },
        time: 60,
        status: "ongoing"
      };

      io.sockets.sockets.get(p1.socketId)?.join(matchId);
      io.sockets.sockets.get(p2.socketId)?.join(matchId);

      io.to(p1.socketId).emit("matchFound", {
        matchId,
        opponent: p2.username,
        isPlayer1: true
      });

      io.to(p2.socketId).emit("matchFound", {
        matchId,
        opponent: p1.username,
        isPlayer1: false
      });

      matchQueue = matchQueue.filter(
        p => p.socketId !== p1.socketId && p.socketId !== p2.socketId
      );
    }
  });

  socket.on("leaveQueue", () => {
    matchQueue = matchQueue.filter(p => p.socketId !== socket.id);
  });

  /* -------- REAL-TIME SCORE SYNC -------- */
  socket.on("scoreUpdate", ({ matchId, score, time }) => {
    const match = activeMatches[matchId];
    if (!match) return;

    match.scores[socket.id] = score;
    match.time = time;

    io.to(matchId).emit("matchUpdate", {
      scores: match.scores,
      players: match.players,
      time: match.time
    });
  });

  /* -------- LEAVE MATCH -------- */
  socket.on("leaveMatch", ({ matchId }) => {
    if (!activeMatches[matchId]) return;

    io.to(matchId).emit("matchEnded");
    delete activeMatches[matchId];
    socket.leave(matchId);
  });

  /* -------- DISCONNECT -------- */
  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);

    matchQueue = matchQueue.filter(p => p.socketId !== socket.id);

    for (const matchId in activeMatches) {
      if (activeMatches[matchId].players[socket.id]) {
        io.to(matchId).emit("matchEnded");
        delete activeMatches[matchId];
      }
    }
  });
});

// ---- Spectator Rooms ----
const spectators = {}; // matchId -> Set of socketIds

io.on("connection", (socket) => {
  // ... existing code

  // Spectator joins a match room
  socket.on("joinSpectate", ({ matchId }) => {
    if (!spectators[matchId]) spectators[matchId] = new Set();
    spectators[matchId].add(socket.id);
    socket.join(`spectate_${matchId}`);
    console.log(`Spectator ${socket.id} joined match ${matchId}`);
  });

  socket.on("leaveSpectate", ({ matchId }) => {
    if (spectators[matchId]) spectators[matchId].delete(socket.id);
    socket.leave(`spectate_${matchId}`);
  });

  // When a player's score updates, notify spectators too
  socket.on("scoreUpdate", ({ matchId, score, time }) => {
    const match = activeMatches[matchId];
    if (!match) return;

    // Update the player score
    const playerName = Object.keys(match.scores).find(name => name !== match.players.find(p => p !== socket.id));
    match.scores[socket.id] = score;
    match.time = time;

    // Send update to both players
    match.players.forEach(p => {
      io.to(p.socketId).emit("matchUpdate", match);
    });

    // Send update to spectators
    io.to(`spectate_${matchId}`).emit("spectateUpdate", match);
  });

  // Send all live matches to spectators on request
  socket.on("requestLiveMatches", () => {
    const liveMatches = Object.values(activeMatches).filter(m => m.status === "ongoing");
    socket.emit("liveMatches", liveMatches);
  });

  socket.on("disconnect", () => {
    // remove from spectator lists
    for (const matchId in spectators) {
      spectators[matchId].delete(socket.id);
    }
  });
});

/* ================= SERVER START ================= */
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
