"""Agent Mesh API Server — standalone FastAPI server for the Agent Mesh Protocol."""

from __future__ import annotations

import asyncio
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
from fastapi.middleware.cors import CORSMiddleware
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
    wait_for_response: bool = False
    response_timeout: float = 30.0


class SendResponse(BaseModel):
    status: str
    id: str
    subject: str
    response: Optional[dict] = None


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

# CORS — allow all origins (internal API on Tailscale)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/mesh/send", response_model=SendResponse)
async def send_raven(req: SendRequest):
    """
    Send a raven to an agent on the mesh.

    Note: This endpoint is designed for internal networks (Tailscale) and has no auth.
    """
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

    if req.wait_for_response:
        # Subscribe to both a unique inbox AND the original reply_to subject
        inbox = f"_INBOX.{msg_id}"
        message.reply_to = inbox

        response_future = asyncio.get_event_loop().create_future()

        async def _on_response(raw_msg) -> None:
            try:
                data = json.loads(raw_msg.data.decode())
                response_msg = MeshMessage.from_json(data)
                if not response_future.done():
                    response_future.set_result(response_msg)
            except Exception:
                pass

        subs = []
        sub1 = await mesh_client.nc.subscribe(inbox, cb=_on_response)
        subs.append(sub1)
        if req.reply_to:
            sub2 = await mesh_client.nc.subscribe(req.reply_to, cb=_on_response)
            subs.append(sub2)

        await mesh_client.publish(message)
        logger.info(
            "Raven %s sent (waiting for response): %s -> %s (%s)",
            msg_id, req.sender, subject, req.msg_type,
        )

        try:
            response = await asyncio.wait_for(
                response_future, timeout=req.response_timeout,
            )
            return SendResponse(
                status="response_received",
                id=msg_id,
                subject=subject,
                response=response.payload,
            )
        except asyncio.TimeoutError:
            logger.info(
                "Raven %s sent but no response received within %.1fs",
                msg_id, req.response_timeout,
            )
            return SendResponse(status="sent_no_response", id=msg_id, subject=subject)
        finally:
            for sub in subs:
                await sub.unsubscribe()

    await mesh_client.publish(message)
    logger.info("Raven %s sent: %s -> %s (%s)", msg_id, req.sender, subject, req.msg_type)
    return SendResponse(status="sent", id=msg_id, subject=subject)


@app.get("/api/mesh/response/{request_id}")
async def get_response(request_id: str):
    """Check the inbox for a response matching a specific request ID."""
    if not INBOX_DIR.exists():
        raise HTTPException(status_code=404, detail="No response found")
    for f in INBOX_DIR.iterdir():
        if f.suffix == ".json":
            try:
                data = json.loads(f.read_text())
                if data.get("reply_to") == request_id:
                    return {"response": data}
            except Exception:
                continue
    raise HTTPException(status_code=404, detail="No response found")


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
