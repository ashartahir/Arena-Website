const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*", // allow all origins (or restrict to your domain)
    methods: ["GET", "POST"]
  }
});

app.use(cors());
app.use(express.json());

// ---- In-memory storage ----
let tournaments = [];
let matchQueue = []; // waiting players
let activeMatches = {}; // matchId -> { players, scores, status }

// ---- Routes ----
app.get("/", (req, res) => {
  res.send("Socket server running");
});

// ---- Socket.IO ----
io.on("connection", (socket) => {
  console.log(`New client connected: ${socket.id}`);

  // --- Tournament Management ---
  socket.on("createTournament", (tournament) => {
    tournaments.push(tournament);
    io.emit("tournamentsUpdate", tournaments);
    console.log("Tournament created:", tournament.name);
  });

  socket.on("deleteTournament", (tournamentId) => {
    tournaments = tournaments.filter(t => t.id !== tournamentId);
    io.emit("tournamentsUpdate", tournaments);
    console.log("Tournament deleted:", tournamentId);
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

  // --- Multiplayer Queue ---
  socket.on("joinQueue", ({ username, gameType }) => {
    console.log(`${username} joined queue for ${gameType}`);
    matchQueue.push({ socketId: socket.id, username, gameType });

    // Try to match players
    const sameTypePlayers = matchQueue.filter(p => p.gameType === gameType);
    if (sameTypePlayers.length >= 2) {
      const player1 = sameTypePlayers[0];
      const player2 = sameTypePlayers[1];

      const matchId = "match_" + Date.now();
      const matchData = {
        id: matchId,
        players: [player1.username, player2.username],
        scores: { [player1.username]: 0, [player2.username]: 0 },
        status: "ongoing"
      };

      activeMatches[matchId] = matchData;

      // Notify both players
      io.to(player1.socketId).emit("matchFound", { opponent: player2.username, matchId, isPlayer1: true });
      io.to(player2.socketId).emit("matchFound", { opponent: player1.username, matchId, isPlayer1: false });

      // Remove from queue
      matchQueue = matchQueue.filter(p => p.socketId !== player1.socketId && p.socketId !== player2.socketId);
    }
  });

  socket.on("leaveQueue", () => {
    matchQueue = matchQueue.filter(p => p.socketId !== socket.id);
  });

  // --- Match Updates ---
  socket.on("updateScore", ({ matchId, username, score }) => {
    const match = activeMatches[matchId];
    if (match) {
      match.scores[username] = score;
      io.to(matchId).emit("matchUpdate", match);
    }
  });

  socket.on("disconnect", () => {
    console.log(`Client disconnected: ${socket.id}`);
    matchQueue = matchQueue.filter(p => p.socketId !== socket.id);

    // Optional: mark active matches for disconnection
  });
});

// ---- Server Start ----
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
