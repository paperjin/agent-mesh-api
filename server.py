"""Agent Mesh API Server — standalone FastAPI server for the Agent Mesh Protocol."""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from agent_mesh import AgentMeshClient, MeshMessage

# ── Config ──────────────────────────────────────────────────────────────────

NATS_URL = os.environ.get("NATS_URL", "nats://100.101.32.70:4222")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8000"))
INBOX_DIR = Path(os.environ.get("MESH_INBOX_DIR", "./data/mesh_inbox"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("mesh_api")

# ── Models ───────────────────────────────────────────────────────────────────

class SendRequest(BaseModel):
    sender: str
    recipient: str
    subject: str
    payload: dict[str, Any]
    msg_type: str = "request"
    reply_to: Optional[str] = None


class SendResponse(BaseModel):
    status: str
    id: str
    subject: str


# ── Globals ──────────────────────────────────────────────────────────────────

mesh_client: Optional[AgentMeshClient] = None


# ── Lifespan ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global mesh_client
    mesh_client = AgentMeshClient("mesh-api", nats_url=NATS_URL)
    await mesh_client.connect()
    logger.info("Mesh API server started — connected to NATS at %s", NATS_URL)
    yield
    if mesh_client:
        await mesh_client.disconnect()
    logger.info("Mesh API server shut down")


# ── App ─────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Agent Mesh API",
    description="Unified HTTP API for sending ravens between AI agents via NATS.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/mesh/send", response_model=SendResponse)
async def send_raven(req: SendRequest):
    if not mesh_client:
        raise HTTPException(status_code=503, detail="NATS not connected")

    msg_id = str(uuid.uuid4())
    subject = req.subject

    # If subject is a bare agent name, expand it to agent.<name>.request
    if "." not in subject and not subject.startswith("agent."):
        subject = f"agent.{subject}.request"

    message = MeshMessage(
        id=msg_id,
        sender=req.sender,
        type=req.msg_type,
        subject=subject,
        payload=req.payload,
        reply_to=req.reply_to,
    )

    await mesh_client.publish(message)
    logger.info("Raven %s sent: %s -> %s (%s)", msg_id, req.sender, subject, req.msg_type)
    return SendResponse(status="sent", id=msg_id, subject=subject)


@app.get("/api/mesh/inbox")
async def list_inbox():
    if not INBOX_DIR.exists():
        return {"messages": []}
    messages = []
    for f in sorted(INBOX_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if f.suffix == ".json":
            try:
                data = json.loads(f.read_text())
                messages.append(data)
            except Exception:
                continue
    return {"messages": messages}


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    logger.info("Starting Mesh API server on %s:%s", HOST, PORT)
    uvicorn.run(
        "server:app",
        host=HOST,
        port=PORT,
        log_level="info",
        reload=False,
    )


if __name__ == "__main__":
    main()
