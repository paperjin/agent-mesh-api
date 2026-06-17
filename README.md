# Agent Mesh API

A standalone HTTP API server for the **Agent Mesh Protocol** — a unified way for AI agents to send messages (ravens) to each other via NATS.

Agents in the mesh: **Llama**, **Bort**, **Oddy** — and any others that speak the protocol.

## Architecture

```
┌─────────────┐     POST /api/mesh/send     ┌──────────────────┐
│   Llama     │ ──────────────────────────► │                  │
│  (client)   │                             │   Mesh API       │
└─────────────┘                             │   Server         │
                                            │  (server.py)     │
┌─────────────┐     POST /api/mesh/send     │                  │
│   Bort      │ ──────────────────────────► │   :8000          │
│  (client)   │                             │                  │
└─────────────┘                             └────────┬─────────┘
                                                     │ publish
                                                     ▼
┌─────────────┐     POST /api/mesh/send     ┌──────────────────┐
│   Oddy      │ ──────────────────────────► │      NATS        │
│  (client)   │                             │   (message bus)  │
└─────────────┘                             └────────┬─────────┘
                                                     │ subscribe
                                                     ▼
                                            ┌──────────────────┐
                                            │   Listener       │
                                            │  (listener.py)   │
                                            │  ./data/mesh_inbox│
                                            └──────────────────┘
```

All agents use the **same endpoint**. The only difference is the hostname (localhost vs Tailscale IP).

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start the server
python server.py
```

The server binds to `0.0.0.0:8000` by default. Verify it's running:

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

## Sending a Raven

### Send a request to Llama

```bash
curl -X POST http://localhost:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{
    "sender": "bort",
    "recipient": "llama",
    "subject": "agent.llama.request",
    "payload": {"query": "What is the meaning of life?"}
  }'
```

### Send a request to Bort

```bash
curl -X POST http://localhost:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{
    "sender": "llama",
    "recipient": "bort",
    "subject": "agent.bort.request",
    "payload": {"task": "summarize the latest logs"}
  }'
```

### Send a request to Oddy

```bash
curl -X POST http://localhost:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{
    "sender": "bort",
    "recipient": "oddy",
    "subject": "agent.oddy.request",
    "payload": {"command": "deploy staging"}
  }'
```

### Broadcast to all agents

```bash
curl -X POST http://localhost:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{
    "sender": "llama",
    "recipient": "all",
    "subject": "agent.broadcast.announcement",
    "payload": {"message": "Maintenance in 5 minutes"}
  }'
```

### Response format

```json
{
  "status": "sent",
  "id": "a1b2c3d4-...",
  "subject": "agent.llama.request"
}
```

## Request-Response Flow

### Send and wait for response in one call

Set `wait_for_response: true` to block until the recipient responds:

```bash
curl -X POST http://localhost:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{
    "sender": "bort",
    "recipient": "llama",
    "subject": "agent.llama.request",
    "payload": {"query": "What is the meaning of life?"},
    "wait_for_response": true,
    "response_timeout": 30.0
  }'
```

If a response arrives in time:

```json
{
  "status": "response_received",
  "id": "a1b2c3d4-...",
  "subject": "agent.llama.request",
  "response": {
    "response": "42 — but you already knew that."
  }
}
```

If the timeout expires:

```json
{
  "status": "sent_no_response",
  "id": "a1b2c3d4-...",
  "subject": "agent.llama.request"
}
```

### Send and poll for response later

Send without waiting, then check for a response by request ID:

```bash
# Step 1: send the raven (note the returned id)
curl -X POST http://localhost:8000/api/mesh/send \
  -H "Content-Type: application/json" \
  -d '{
    "sender": "bort",
    "recipient": "llama",
    "subject": "agent.llama.request",
    "payload": {"query": "What is the meaning of life?"}
  }'
# → {"status":"sent","id":"abc-123","subject":"agent.llama.request"}

# Step 2: poll for the response using the request id
curl http://localhost:8000/api/mesh/response/abc-123
# → {"response":{"id":"...","reply_to":"abc-123","payload":{...}}}
```

### Full request-response flow

1. **Llama** sends a raven to **Bort** with `wait_for_response: true`
2. The Mesh API publishes the raven to `agent.bort.request` on NATS
3. The **Listener** (running with `--respond --agent bort`) receives the raven
4. The Listener spawns `hermes chat -q -- "<prompt>"` to generate a response
5. The Listener publishes the response back to the inbox subject
6. The Mesh API receives the response and returns it inline to Llama

## Reading the Inbox

```bash
curl http://localhost:8000/api/mesh/inbox
```

Returns a list of received messages sorted newest-first.

## Running the Listener

The listener subscribes to mesh subjects and saves incoming ravens to disk.

```bash
# Basic listener (saves messages to ./data/mesh_inbox/)
python listener.py

# Listener with auto-response (spawns hermes chat -q for requests)
python listener.py --respond
```

## Environment Variables

| Variable        | Default                        | Description                    |
|-----------------|--------------------------------|--------------------------------|
| `NATS_URL`      | `nats://100.101.32.70:4222`    | NATS server address            |
| `HOST`          | `0.0.0.0`                      | Server bind address            |
| `PORT`          | `8000`                         | Server port                    |
| `MESH_INBOX_DIR`| `./data/mesh_inbox`            | Directory for received messages|
| `MESH_PID_FILE` | `./mesh_listener.pid`          | PID file path (listener only)  |

## Deploying as a Service

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

### Windows (NSSM)

```powershell
nssm install AgentMeshApi "C:\Python311\python.exe" "C:\agent-mesh-api\server.py"
nssm set AgentMeshApi AppDirectory "C:\agent-mesh-api"
nssm set AgentMeshApi AppStdout "C:\agent-mesh-api\mesh-api.log"
nssm set AgentMeshApi AppStderr "C:\agent-mesh-api\mesh-api.log"
nssm start AgentMeshApi
```

## License

MIT
