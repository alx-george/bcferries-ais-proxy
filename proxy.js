/**
 * proxy.js
 * --------
 * WebSocket proxy — runs on Render as a free Web Service.
 * Browser connects to this proxy via WebSocket.
 * Proxy connects to aisstream.io and forwards BC Ferries positions.
 *
 * Environment variables (set in Render dashboard):
 *   AIS_API_KEY   — your aisstream.io API key
 *   PORT          — set automatically by Render
 */

const http      = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

// All BC Ferries MMSIs
const BCF_MMSIS = [
  "316001649","316001650",
  "316017849","316017850","316017848",
  "316001653","316001655","316001656","316001660","316001661",
  "316022501","316022502","316022503","316022504","316022505",
  "316024001","316024002","316024003","316024004",
  "316014601","316001690",
  "316001651","316001654","316001658","316001659",
  "316001662","316001663","316001664","316001665",
  "316001666","316001667","316001668","316001669",
  "316001652","316001657",
];

const NAV_STATUS = {
  0:"Under way using engine", 1:"At anchor", 2:"Not under command",
  3:"Restricted manoeuvrability", 4:"Constrained by draught",
  5:"Moored", 6:"Aground", 7:"Engaged in fishing",
  8:"Under way sailing", 15:"Not defined",
};

// ---------------------------------------------------------------------------
// HTTP server — Render needs an HTTP endpoint to confirm the service is up
// ---------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status:  "ok",
    service: "BC Ferries AIS Proxy",
    vessels: Object.keys(connectedVessels).length,
    clients: wss.clients.size,
  }));
});

// ---------------------------------------------------------------------------
// WebSocket server — browsers connect here
// ---------------------------------------------------------------------------
const wss = new WebSocket.Server({ server });

// Track latest vessel positions (mmsi -> data)
// This means a new browser tab gets current positions immediately
const connectedVessels = {};

// Single shared aisstream connection (reused across all browser clients)
let aisSocket     = null;
let aisConnecting = false;
let reconnectTimer = null;

function connectToAisstream() {
  if (aisConnecting || (aisSocket && aisSocket.readyState === WebSocket.OPEN)) return;

  const apiKey = process.env.AIS_API_KEY;
  if (!apiKey) {
    console.error("AIS_API_KEY not set — cannot connect to aisstream");
    return;
  }

  aisConnecting = true;
  console.log(`[${new Date().toISOString()}] Connecting to aisstream.io...`);

  aisSocket = new WebSocket("wss://stream.aisstream.io/v0/stream");

  aisSocket.on("open", () => {
    aisConnecting = false;
    console.log(`[${new Date().toISOString()}] Connected to aisstream. Subscribing...`);
    aisSocket.send(JSON.stringify({
      APIkey:             apiKey,
      BoundingBoxes:      [[[47.5, -133.0], [55.5, -122.0]]],
      FiltersShipMMSI:    BCF_MMSIS,
      FilterMessageTypes: ["PositionReport", "StandardClassBPositionReport"],
    }));
  });

  aisSocket.on("message", (raw) => {
    try {
      const msg     = JSON.parse(raw);
      const msgType = msg.MessageType;
      if (!msgType) return;

      const meta = msg.MetaData || {};
      const body = (msg.Message || {})[msgType] || {};
      const mmsi = parseInt(meta.MMSI || body.UserId || 0);
      if (!mmsi) return;

      const lat = meta.latitude  ?? body.Latitude;
      const lon = meta.longitude ?? body.Longitude;
      if (lat == null || lon == null) return;
      if (lat === 0 && lon === 0)     return;

      let sog     = body.Sog;
      let cog     = body.Cog;
      let heading = body.TrueHeading;
      if (sog     > 102) sog     = null;
      if (cog     > 360) cog     = null;
      if (heading > 360) heading = null;

      const vessel = {
        mmsi,
        name:       (meta.ShipName || "").trim() || null,
        lat:        +lat.toFixed(6),
        lon:        +lon.toFixed(6),
        sog:        sog     != null ? +sog.toFixed(1)     : null,
        cog:        cog     != null ? +cog.toFixed(1)     : null,
        heading:    heading != null ? Math.round(heading) : null,
        nav_status: NAV_STATUS[body.NavigationalStatus] || "Unknown",
        timestamp:  new Date().toISOString(),
      };

      // Cache latest position
      connectedVessels[mmsi] = vessel;

      // Forward to all connected browser clients
      const payload = JSON.stringify({ type: "position", vessel });
      wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(payload);
        }
      });

      console.log(`  [${new Date().toISOString()}] ${vessel.name || mmsi} @ ${vessel.lat},${vessel.lon} SOG ${vessel.sog}`);

    } catch (e) {
      // Ignore malformed messages
    }
  });

  aisSocket.on("close", (code, reason) => {
    aisConnecting = false;
    console.log(`[${new Date().toISOString()}] aisstream disconnected (${code}). Reconnecting in 10s...`);
    // Notify all browsers
    const payload = JSON.stringify({ type: "status", status: "reconnecting" });
    wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(payload); });
    reconnectTimer = setTimeout(connectToAisstream, 10000);
  });

  aisSocket.on("error", (err) => {
    aisConnecting = false;
    console.error(`[${new Date().toISOString()}] aisstream error: ${err.message}`);
  });
}

// ---------------------------------------------------------------------------
// Handle browser WebSocket connections
// ---------------------------------------------------------------------------
wss.on("connection", (browserSocket, req) => {
  const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
  console.log(`[${new Date().toISOString()}] Browser connected from ${ip}. Total clients: ${wss.clients.size}`);

  // Send current cached positions immediately so map populates instantly
  if (Object.keys(connectedVessels).length > 0) {
    browserSocket.send(JSON.stringify({
      type:    "snapshot",
      vessels: Object.values(connectedVessels),
    }));
  }

  // Send current status
  const isConnected = aisSocket && aisSocket.readyState === WebSocket.OPEN;
  browserSocket.send(JSON.stringify({
    type:   "status",
    status: isConnected ? "connected" : "connecting",
  }));

  // Start aisstream connection if not already running
  connectToAisstream();

  browserSocket.on("close", () => {
    console.log(`[${new Date().toISOString()}] Browser disconnected. Remaining clients: ${wss.clients.size}`);
  });

  browserSocket.on("error", () => {});
});

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------
server.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] BC Ferries AIS Proxy listening on port ${PORT}`);
  console.log(`  AIS_API_KEY: ${process.env.AIS_API_KEY ? "set" : "NOT SET"}`);
});
