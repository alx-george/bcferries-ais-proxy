/**
 * proxy.js
 * --------
 * WebSocket proxy for BC Ferries live vessel tracking.
 * Runs on Render as a free Web Service.
 *
 * What it does:
 *   1. On boot: seeds in-memory cache from Supabase (instant data for first visitor)
 *   2. Connects to aisstream.io and subscribes to BCF vessel positions
 *   3. Forwards live positions to all connected browser WebSocket clients
 *   4. Upserts positions to Supabase every 30 seconds (persistence layer)
 *   5. Auto-reconnects to aisstream if connection drops
 *   6. Keep-alive ping prevents aisstream closing idle connections
 *
 * Environment variables (set in Render dashboard):
 *   AIS_API_KEY    — aisstream.io API key
 *   SUPABASE_URL   — https://xxxx.supabase.co
 *   SUPABASE_KEY   — service_role secret key (NOT anon key)
 *   PORT           — set automatically by Render
 */

const http      = require("http");
const https     = require("https");
const WebSocket = require("ws");

const PORT                  = process.env.PORT         || 3000;
const AIS_API_KEY           = process.env.AIS_API_KEY  || "";
const SUPABASE_URL          = process.env.SUPABASE_URL || "";
const SUPABASE_KEY          = process.env.SUPABASE_KEY || "";
const AIS_WS_URL            = "wss://stream.aisstream.io/v0/stream";
const SUPABASE_TABLE        = "vessel_positions";
const UPSERT_INTERVAL_MS    = 30000;
const RECONNECT_DELAY_MS    = 10000;
const KEEPALIVE_INTERVAL_MS = 25000;

// ---------------------------------------------------------------------------
// BCF MMSI list — verified current fleet May 2026
// ---------------------------------------------------------------------------
const BCF_MMSIS = [
  "316001247",  // Queen of Capilano
  "316001249",  // Queen of Coquitlam
  "316001248",  // Queen of Cowichan
  "316001252",  // Queen of Cumberland
  "316001255",  // Queen of New Westminster
  "316001257",  // Queen of Oak Bay
  "316001262",  // Queen of Surrey
  "316001265",  // Quinitsa
  "316001266",  // Quinsam
  "316030626",  // Salish Eagle
  "316047943",  // Salish Heron
  "316030627",  // Salish Orca
  "316030628",  // Salish Raven
  "316001267",  // Skeena Queen
  "316001268",  // Spirit of British Columbia
  "316001269",  // Spirit of Vancouver Island
  "316001271",  // Tachek
  "316030644",  // Baynes Sound Connector
  "316011409",  // Coastal Celebration
  "316011408",  // Coastal Inspiration
  "316011407",  // Coastal Renaissance
  "316039863",  // Island Aurora
  "316039864",  // Island Discovery
  "316047946",  // Island Gwawis
  "316047555",  // Island Kwigwis
  "316046934",  // Island K'ulut'a
  "316046819",  // Island Nagalis
  "316001236",  // Kahloke
  "316001235",  // Klitsa
  "316001237",  // Kwuna
  "316012774",  // Malaspina Sky
  "316194000",  // Northern Adventure
  "316014054",  // Northern Expedition
  "316036676",  // Northern Sea Wolf
  "316009547",  // Pune'luxutth
  "316001244",  // Quadra Queen II
  "316001245",  // Queen of Alberni
];

const NAV_STATUS = {
  0:"Under way using engine", 1:"At anchor", 2:"Not under command",
  3:"Restricted manoeuvrability", 4:"Constrained by draught",
  5:"Moored", 6:"Aground", 7:"Engaged in fishing",
  8:"Under way sailing", 15:"Not defined",
};

// ---------------------------------------------------------------------------
// In-memory vessel cache  { mmsi -> vessel object }
// ---------------------------------------------------------------------------
const vesselCache = {};

// ---------------------------------------------------------------------------
// Supabase helpers — raw HTTPS, no SDK needed
// ---------------------------------------------------------------------------
function supabaseRequest(method, path, body) {
  return new Promise((resolve) => {
    if (!SUPABASE_URL || !SUPABASE_KEY) { resolve(null); return; }
    const url     = new URL(SUPABASE_URL);
    const options = {
      hostname: url.hostname,
      path:     `/rest/v1/${path}`,
      method,
      headers: {
        "apikey":        SUPABASE_KEY,
        "Authorization": `Bearer ${SUPABASE_KEY}`,
        "Content-Type":  "application/json",
        "Prefer":        method === "POST" ? "resolution=merge-duplicates" : "",
      },
    };
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", chunk => data += chunk);
      res.on("end", () => {
        try { resolve(data ? JSON.parse(data) : null); }
        catch { resolve(null); }
      });
    });
    req.on("error", (e) => {
      console.error(`Supabase error: ${e.message}`);
      resolve(null);
    });
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function seedFromSupabase() {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.log("Supabase not configured — starting with empty cache");
    return;
  }
  console.log("Seeding vessel cache from Supabase...");
  const rows = await supabaseRequest("GET", `${SUPABASE_TABLE}?select=*&limit=100`);
  if (Array.isArray(rows) && rows.length > 0) {
    rows.forEach(row => { vesselCache[row.mmsi] = row; });
    console.log(`Seeded ${rows.length} vessels from Supabase`);
  } else {
    console.log("Supabase empty — starting with empty cache");
  }
}

async function upsertToSupabase() {
  const vessels = Object.values(vesselCache);
  if (!vessels.length || !SUPABASE_URL || !SUPABASE_KEY) return;
  const rows = vessels.map(v => ({
    mmsi:       v.mmsi,
    name:       v.name,
    lat:        v.lat,
    lon:        v.lon,
    sog:        v.sog,
    cog:        v.cog,
    heading:    v.heading,
    nav_status: v.nav_status,
    updated_at: v.timestamp || new Date().toISOString(),
  }));
  await supabaseRequest("POST", `${SUPABASE_TABLE}?on_conflict=mmsi`, rows);
  console.log(`[${new Date().toISOString()}] Upserted ${rows.length} vessels to Supabase`);
}

// ---------------------------------------------------------------------------
// HTTP server — health check + status page
// ---------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Content-Type", "application/json");
  res.writeHead(200);
  res.end(JSON.stringify({
    status:        "ok",
    service:       "BC Ferries AIS Proxy",
    vessels:       Object.keys(vesselCache).length,
    clients:       wss ? wss.clients.size : 0,
    ais_connected: aisSocket ? aisSocket.readyState === WebSocket.OPEN : false,
    uptime_s:      Math.floor(process.uptime()),
    updated_at:    new Date().toISOString(),
  }, null, 2));
});

// ---------------------------------------------------------------------------
// WebSocket server — browser clients connect here
// ---------------------------------------------------------------------------
const wss = new WebSocket.Server({ server });

wss.on("connection", (browserSocket, req) => {
  const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress || "unknown";
  console.log(`[${new Date().toISOString()}] Browser connected (${ip}) — clients: ${wss.clients.size}`);

  // Send snapshot of all cached positions immediately
  const cached = Object.values(vesselCache);
  browserSocket.send(JSON.stringify({
    type:       "snapshot",
    vessels:    cached,
    count:      cached.length,
    updated_at: new Date().toISOString(),
  }));

  // Send AIS connection status
  browserSocket.send(JSON.stringify({
    type:   "status",
    status: aisSocket && aisSocket.readyState === WebSocket.OPEN ? "connected" : "connecting",
  }));

  connectToAisstream();

  browserSocket.on("close", () => {
    console.log(`[${new Date().toISOString()}] Browser disconnected — remaining: ${wss.clients.size}`);
  });
  browserSocket.on("error", () => {});
});

// ---------------------------------------------------------------------------
// aisstream.io connection with auto-reconnect
// ---------------------------------------------------------------------------
let aisSocket      = null;
let aisConnecting  = false;
let reconnectTimer = null;
let keepaliveTimer = null;

function connectToAisstream() {
  if (aisConnecting || (aisSocket && aisSocket.readyState === WebSocket.OPEN)) return;
  if (!AIS_API_KEY) { console.error("AIS_API_KEY not set"); return; }

  clearTimeout(reconnectTimer);
  aisConnecting = true;
  console.log(`[${new Date().toISOString()}] Connecting to aisstream.io...`);

  aisSocket = new WebSocket(AIS_WS_URL);

  aisSocket.on("open", () => {
    aisConnecting = false;
    console.log(`[${new Date().toISOString()}] Connected. Subscribing to ${BCF_MMSIS.length} BCF vessels...`);

    aisSocket.send(JSON.stringify({
      APIkey:             AIS_API_KEY,
      BoundingBoxes:      [[[47.5, -133.0], [55.5, -122.0]]],
      FiltersShipMMSI:    BCF_MMSIS,
      FilterMessageTypes: ["PositionReport", "StandardClassBPositionReport"],
    }));

    // Keep-alive ping every 25s
    clearInterval(keepaliveTimer);
    keepaliveTimer = setInterval(() => {
      if (aisSocket && aisSocket.readyState === WebSocket.OPEN) aisSocket.ping();
    }, KEEPALIVE_INTERVAL_MS);

    broadcast({ type: "status", status: "connected" });
  });

  aisSocket.on("message", (raw) => {
    try {
      const msg     = JSON.parse(raw);
      const msgType = msg.MessageType;
      if (!msgType) return;

      const meta = msg.MetaData  || {};
      const body = (msg.Message  || {})[msgType] || {};
      const mmsi = parseInt(meta.MMSI || body.UserId || 0);
      if (!mmsi) return;

      const lat = meta.latitude  ?? body.Latitude;
      const lon = meta.longitude ?? body.Longitude;
      if (lat == null || lon == null) return;
      if (lat === 0   && lon === 0)   return;
      if (lat < 47    || lat > 56)    return;
      if (lon < -134  || lon > -121)  return;

      let sog     = body.Sog;
      let cog     = body.Cog;
      let heading = body.TrueHeading;
      if (sog     != null && sog     > 102) sog     = null;
      if (cog     != null && cog     > 360) cog     = null;
      if (heading != null && heading > 360) heading = null;

      const vessel = {
        mmsi,
        name:       (meta.ShipName || "").trim().replace(/\s+/g, " ") || null,
        lat:        +lat.toFixed(6),
        lon:        +lon.toFixed(6),
        sog:        sog     != null ? +sog.toFixed(1)     : null,
        cog:        cog     != null ? +cog.toFixed(1)     : null,
        heading:    heading != null ? Math.round(heading) : null,
        nav_status: NAV_STATUS[body.NavigationalStatus ?? 15] || "Unknown",
        timestamp:  new Date().toISOString(),
      };

      vesselCache[mmsi] = vessel;
      broadcast({ type: "position", vessel });

      console.log(`  ${String(vessel.name || mmsi).padEnd(34)} @ ${vessel.lat},${vessel.lon}  SOG ${vessel.sog ?? "--"}`);

    } catch (e) { /* ignore malformed */ }
  });

  aisSocket.on("pong", () => { /* alive */ });

  aisSocket.on("close", (code) => {
    aisConnecting = false;
    clearInterval(keepaliveTimer);
    console.log(`[${new Date().toISOString()}] aisstream disconnected (${code}). Reconnecting in ${RECONNECT_DELAY_MS / 1000}s...`);
    broadcast({ type: "status", status: "reconnecting" });
    reconnectTimer = setTimeout(connectToAisstream, RECONNECT_DELAY_MS);
  });

  aisSocket.on("error", (err) => {
    aisConnecting = false;
    clearInterval(keepaliveTimer);
    console.error(`[${new Date().toISOString()}] aisstream error: ${err.message}`);
  });
}

function broadcast(obj) {
  const payload = JSON.stringify(obj);
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(payload);
  });
}

// ---------------------------------------------------------------------------
// Supabase upsert on interval
// ---------------------------------------------------------------------------
setInterval(upsertToSupabase, UPSERT_INTERVAL_MS);

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
async function boot() {
  console.log("=".repeat(60));
  console.log("BC Ferries AIS Proxy");
  console.log("=".repeat(60));
  console.log(`AIS_API_KEY  : ${AIS_API_KEY  ? "set" : "NOT SET -- proxy will not receive data"}`);
  console.log(`SUPABASE_URL : ${SUPABASE_URL || "NOT SET -- no persistence"}`);
  console.log(`SUPABASE_KEY : ${SUPABASE_KEY ? "set" : "NOT SET -- no persistence"}`);
  console.log("=".repeat(60));

  await seedFromSupabase();

  server.listen(PORT, () => {
    console.log(`[${new Date().toISOString()}] Listening on port ${PORT}`);
  });

  connectToAisstream();
}

boot();
