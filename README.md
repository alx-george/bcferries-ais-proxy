# bcferries-ais-proxy

WebSocket proxy for BC Ferries live vessel tracking.
Deployed on Render as a free Web Service.

## How it works

1. Browser connects to this proxy via WebSocket
2. Proxy connects to aisstream.io and subscribes to BC Ferries vessel positions
3. Positions are forwarded to the browser in real time
4. Latest positions are cached — new browser tabs get current data instantly

## Deploy on Render

1. Push this repo to GitHub
2. Render dashboard → New → Web Service → connect repo
3. Settings:
   - Build command: `npm install`
   - Start command: `node proxy.js`
4. Environment variable: `AIS_API_KEY` = your aisstream.io key
5. Deploy

## Message types sent to browser

### snapshot (on connect — current cached positions)
```json
{
  "type": "snapshot",
  "vessels": [{ "mmsi": 316001649, "name": "Spirit Of British Columbia", "lat": 48.92, "lon": -123.41, "sog": 18.4, "cog": 214.0, "heading": 216, "nav_status": "Under way using engine", "timestamp": "2025-05-01T12:00:00.000Z" }]
}
```

### position (real-time update)
```json
{
  "type": "position",
  "vessel": { "mmsi": 316001649, "name": "Spirit Of British Columbia", "lat": 48.93, "lon": -123.42, "sog": 18.6, "cog": 215.0, "heading": 217, "nav_status": "Under way using engine", "timestamp": "2025-05-01T12:00:05.000Z" }
}
```

### status
```json
{ "type": "status", "status": "connected" }
{ "type": "status", "status": "reconnecting" }
```
