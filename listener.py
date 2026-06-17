#!/usr/bin/env python3
"""Agent Mesh Listener — subscribes to mesh subjects and saves ravens to disk."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from agent_mesh import AgentMeshClient, MeshMessage

# ── Config ──────────────────────────────────────────────────────────────────

NATS_URL = os.environ.get("NATS_URL", "nats://100.101.32.70:4222")
INBOX_DIR = Path(os.environ.get("MESH_INBOX_DIR", "./data/mesh_inbox"))
PID_FILE = Path(os.environ.get("MESH_PID_FILE", "./mesh_listener.pid"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("mesh_listener")


# ── Listener ────────────────────────────────────────────────────────────────

class MeshListener:
    """Listens for mesh messages and saves them to the inbox directory."""

    def __init__(self, respond: bool = False):
        self.respond = respond
        self.client = AgentMeshClient("mesh-listener", nats_url=NATS_URL)
        self._running = False

    async def start(self) -> None:
        self._running = True
        INBOX_DIR.mkdir(parents=True, exist_ok=True)

        await self.client.connect()

        # Subscribe to all agent request subjects
        await self.client.subscribe("agent.*.request", self._on_message)
        # Subscribe to all broadcast subjects
        await self.client.subscribe("agent.broadcast.>", self._on_message)
        # Subscribe to all heartbeat subjects
        await self.client.subscribe("agent.heartbeat.>", self._on_message)

        logger.info("Listener started — watching for ravens")

        # Write PID file
        PID_FILE.write_text(str(os.getpid()))
        logger.info("PID written to %s", PID_FILE)

        # Handle shutdown signals
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        # Keep running
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        logger.info("Shutting down listener...")
        self._running = False
        await self.client.disconnect()
        if PID_FILE.exists():
            PID_FILE.unlink()

    async def _on_message(self, msg: MeshMessage) -> None:
        """Handle an incoming mesh message."""
        # Save to inbox
        filename = f"{msg.id}.json"
        filepath = INBOX_DIR / filename
        filepath.write_text(json.dumps(msg.to_json(), indent=2))
        logger.info("Saved raven %s to %s", msg.id, filepath)

        # If --respond and this is a request, spawn hermes to handle it
        if self.respond and msg.type == "request":
            asyncio.create_task(self._handle_request(msg))

    async def _handle_request(self, msg: MeshMessage) -> None:
        """Spawn hermes chat to respond to a request message."""
        logger.info("Spawning hermes for raven %s", msg.id)
        try:
            proc = await asyncio.create_subprocess_exec(
                "hermes", "chat", "-q",
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            input_text = json.dumps(msg.payload, indent=2)
            stdout, stderr = await proc.communicate(input_text.encode(), timeout=60)

            if proc.returncode != 0:
                logger.error("hermes failed for %s: %s", msg.id, stderr.decode())
                return

            response_payload = {"response": stdout.decode().strip()}
            reply = MeshMessage(
                id=str(uuid.uuid4()),
                sender="mesh-listener",
                type="response",
                subject=msg.reply_to or f"agent.{msg.sender}.response",
                payload=response_payload,
                reply_to=msg.id,
            )
            await self.client.publish(reply)
            logger.info("Response sent for raven %s", msg.id)

        except asyncio.TimeoutError:
            logger.error("hermes timed out for %s", msg.id)
        except FileNotFoundError:
            logger.error("hermes not found — is it installed and on PATH?")
        except Exception as e:
            logger.error("Error handling request %s: %s", msg.id, e)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Agent Mesh NATS Listener")
    parser.add_argument(
        "--respond",
        action="store_true",
        help="Spawn hermes chat to respond to request messages",
    )
    args = parser.parse_args()

    listener = MeshListener(respond=args.respond)
    try:
        asyncio.run(listener.start())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
