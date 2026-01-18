
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');



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
app.use(express.static(path.join(__dirname, 'public')));
/* ================= IN-MEMORY STORAGE ================= */
let tournaments = [];
let matchQueue = [];           // waiting players
let activeMatches = {};        // matchId → match object
const spectators = {};         // matchId → Set of socketIds

/* ================= ROUTES ================= */
app.get("/", (req, res) => {
  res.send("Socket server running");
});

/* ================= SOCKET.IO ================= */
io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  /* ================= TOURNAMENTS ================= */
  socket.on("createTournament", (tournament) => {
    tournaments.push(tournament);
    io.emit("tournamentsUpdate", tournaments);
  });

  socket.on("deleteTournament", (tournamentId) => {
    tournaments = tournaments.filter(t => t.id !== tournamentId);
    io.emit("tournamentsUpdate", tournaments);
  });
socket.on("connect", () => {
    console.log("Socket connected:", socket.id);
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

  /* ================= MATCHMAKING ================= */
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

  /* ================= REAL-TIME SCORE SYNC ================= */
  socket.on("scoreUpdate", ({ matchId, score, time }) => {
    const match = activeMatches[matchId];
    if (!match) return;

    match.scores[socket.id] = score;
    match.time = time;

    const scoreByName = {};
    Object.entries(match.players).forEach(([sid, name]) => {
      scoreByName[name] = match.scores[sid] || 0;
    });

    io.to(matchId).emit("matchUpdate", {
      scores: scoreByName,
      players: Object.values(match.players),
      time: match.time
    });

    // Update spectators if any
    if (spectators[matchId]) {
      io.to(`spectate_${matchId}`).emit("spectateUpdate", {
        id: matchId,
        scores: Object.fromEntries(
          Object.entries(match.players).map(([sid, name]) => [name, match.scores[sid] || 0])
        ),
        players: Object.values(match.players),
        time: match.time
      });
    }
  });

  /* ================= LEAVE MATCH ================= */
  socket.on("leaveMatch", ({ matchId }) => {
    const match = activeMatches[matchId];
    if (!match) return;

    io.to(matchId).emit("matchEnded", {
      scores: match.scores,
      players: match.players
    });

    // Remove match
    delete activeMatches[matchId];

    // Remove all players from the room
    Object.keys(match.players).forEach(id => {
      io.sockets.sockets.get(id)?.leave(matchId);
    });
  });

  /* ================= SPECTATOR ================= */
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

  socket.on("requestLiveMatches", () => {
    const liveMatches = Object.values(activeMatches)
      .filter(m => m.status === "ongoing")
      .map(m => ({
        id: m.matchId,
        players: Object.values(m.players),
        scores: Object.fromEntries(
          Object.entries(m.players).map(([id, name]) => [name, m.scores[id] || 0])
        ),
        time: m.time
      }));
    socket.emit("liveMatches", liveMatches);
  });

  /* ================= DISCONNECT ================= */
  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);

    // Remove from queue
    matchQueue = matchQueue.filter(p => p.socketId !== socket.id);

    // Remove from spectators
    for (const matchId in spectators) {
      spectators[matchId].delete(socket.id);
    }

    // End matches if a player disconnects
    for (const matchId in activeMatches) {
      if (activeMatches[matchId].players[socket.id]) {
        io.to(matchId).emit("matchEnded", {
          scores: activeMatches[matchId].scores,
          players: activeMatches[matchId].players
        });
        delete activeMatches[matchId];
      }
    }
  });
});

/* ================= SERVER START ================= */
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
