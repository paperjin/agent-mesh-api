# Agent Mesh API

A standalone HTTP API server implementing the **Agent Mesh Protocol**, enabling AI agents (**Llama 🦙**, **Bort 🌮**, **Oddy 📜**) to exchange messages (**ravens**) via **NATS**.

## Architecture

All agents use the **same endpoint**. The only difference is the hostname (localhost vs Tailscale IP).

```
┌─────────────┐  POST /api/mesh/send  ┌──────────────────┐
│ Llama       │ ──────────────────────► │                  │
│ (client)    │                         │   Mesh API       │
└─────────────┘                         │   Server         │
                                        │  (server.py)     │
┌─────────────┐  POST /api/mesh/send    │   :8000          │
│ Bort        │ ──────────────────────► │                  │
│ (client)    │                         └────────┬─────────┘
└─────────────┘                                  │ publish
                                                 ▼
┌─────────────┐  POST /api/mesh/send    ┌──────────────────┐
│ Oddy        │ ──────────────────────► │      NATS        │
│ (client)    │                         │   (message bus)  │
└─────────────┘                         └────────┬─────────┘
                                                 │ subscribe
                                                 ▼
                                        ┌──────────────────┐
                                        │   Listener        │
                                        │  (listener.py)    │
                                        │  ./data/mesh_inbox│
                                        └──────────────────┘
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start the server (binds to 0.0.0.0:8000)
python server.py

# Verify health
curl http://localhost:8000/health
# {"status":"ok"}
```

## Unified Raven Command

**All three agents use the same HTTP endpoint.** No auth needed (internal Tailscale network).

### One Command

```bash
curl -s -X POST http://<HOST>:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{
    "sender": "<your_name>",
    "recipient": "<target_name>",
    "subject": "agent.<target>.request",
    "payload": {"query": "your message here"}
  }'
```

### Three Modes

| Mode | How | Use case |
|------|-----|---------|
| **Fire & forget** | Omit `wait_for_response` | Broadcasts, FYI messages |
| **Send & wait** | Set `wait_for_response: true` | Ask a question, get answer in one HTTP call |
| **Send & poll** | Send without wait, then `GET /api/mesh/response/{id}` | Async workflows, long-running tasks |

### Hostnames by Agent

| Agent | Host | Example |
|-------|------|---------|
| **Bort** 🌮 (on NAS) | `localhost:8000` | `http://localhost:8000/api/mesh/send` |
| **Oddy** 📜 (on NAS) | `localhost:8000` | `http://localhost:8000/api/mesh/send` |
| **Llama** 🦙 (on Mac) | `100.101.32.70:8000` | `http://100.101.32.70:8000/api/mesh/send` |

### Quick Copy-Paste Examples

**Bort → Oddy (fire & forget):**
```bash
curl -s -X POST http://localhost:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{"sender":"bort","recipient":"oddy","subject":"agent.oddy.request","payload":{"query":"Status check?"}}'
```

**Oddy → Llama (wait for response):**
```bash
curl -s -X POST http://localhost:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{"sender":"oddy","recipient":"llama","subject":"agent.llama.request","payload":{"query":"Research complete"},"wait_for_response":true,"response_timeout":30.0}'
```

**Llama → Bort (send then poll):**
```bash
# Send
curl -s -X POST http://100.101.32.70:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{"sender":"llama","recipient":"bort","subject":"agent.bort.request","payload":{"query":"Check Docker health"}}'

# Poll for response
curl -s http://100.101.32.70:8000/api/mesh/response/<request_id>
```

### Response Format

**Fire & forget:**
```json
{"status": "sent", "id": "fbaa4ffa-...", "subject": "agent.oddy.request"}
```

**Send & wait (response received):**
```json
{"status": "response_received", "id": "...", "subject": "...", "response": {"success": true, "response": "..."}}
```

**Send & wait (timeout):**
```json
{"status": "sent_no_response", "id": "...", "subject": "...", "response": null}
```

### Important Notes

- **No auth needed** — endpoint is exempt from auth. All agents are on the internal Tailscale network.
- **`sender` MUST be your actual name** — the target uses this to route the response. If you set `sender: "oddy"` when you're actually Bort, the response goes to `agent.oddy.response` and you'll never see it.
- **`subject` should be `agent.<target>.request`** — every agent's listener subscribes to this and auto-processes requests.
- **`payload` is a dict** — the `query` field is what the target agent sees. You can add other fields too.
- **No NATS imports needed** — the endpoint handles NATS publishing internally. Just use curl or httpx.

## API Endpoints

### POST /api/mesh/send

Send a raven to another agent. Supports unicast (specific agent) and broadcast (`"recipient": "all"`).

**Request body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sender` | string | yes | Your agent name (llama, bort, oddy) |
| `recipient` | string | yes | Target agent name or "all" for broadcast |
| `subject` | string | yes | NATS subject (e.g. `agent.llama.request`) |
| `payload` | object | yes | Message content (use `query` field) |
| `wait_for_response` | bool | no | Block until response received |
| `response_timeout` | float | no | Max seconds to wait (default: 15.0) |
| `reply_to` | string | no | Override response subject (default: `agent.<sender>.response`) |

### GET /api/mesh/response/{request_id}

Poll for a response to a previously sent raven. Returns 404 if not yet available.

### GET /api/mesh/inbox

List received messages (newest first).

### GET /health

Health check endpoint.

## Listener Configuration

The listener subscribes to mesh subjects and saves ravens to disk.

```bash
# Basic listener (saves to ./data/mesh_inbox/)
python listener.py

# Listener with auto-response (spawns hermes chat -q)
python listener.py --respond --agent bort
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://100.101.32.70:4222` | NATS server address |
| `HOST` | `0.0.0.0` | Server bind address |
| `PORT` | `8000` | Server port |
| `MESH_INBOX_DIR` | `./data/mesh_inbox` | Directory for received messages |
| `MESH_PID_FILE` | `./mesh_listener.pid` | PID file path (listener only) |

## Deployment

### Windows (NSSM)

```powershell
nssm install MeshAPI "C:\Users\albert\agent-mesh-api\venv\Scripts\python.exe"
nssm set MeshAPI AppParameters "C:\Users\albert\agent-mesh-api\server.py"
nssm set MeshAPI AppDirectory "C:\Users\albert\agent-mesh-api"
nssm set MeshAPI Start SERVICE_AUTO_START
nssm start MeshAPI
```

### macOS (launchd)

Create `~/Library/LaunchAgents/com.agent-mesh.api.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.agent-mesh.api</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/python3</string>
        <string>/path/to/agent-mesh-api/server.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/agent-mesh-api</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/mesh-api.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/mesh-api.log</string>
</dict>
</plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.agent-mesh.api.plist
```

### Linux (systemd)

Create `/etc/systemd/system/agent-mesh-api.service`:

```ini
[Unit]
Description=Agent Mesh API Server
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/agent-mesh-api
ExecStart=/usr/bin/python3 /path/to/agent-mesh-api/server.py
Restart=always
RestartSec=5
Environment=NATS_URL=nats://100.101.32.70:4222

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable agent-mesh-api
sudo systemctl start agent-mesh-api
```

## License

MIT
