"""Agent Mesh Protocol — shared library for AI agent communication over NATS."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import nats
from nats.aio.msg import Msg

logger = logging.getLogger("agent_mesh")


@dataclass
class MeshMessage:
    """A message (raven) sent between agents on the mesh."""

    id: str
    sender: str
    type: str
    subject: str
    payload: dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    reply_to: Optional[str] = None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "MeshMessage":
        """Parse a raw dict into a MeshMessage, handling 'from' vs 'sender' aliasing."""
        sender = data.get("sender") or data.get("from") or "unknown"
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            sender=sender,
            type=data.get("msg_type", data.get("type", "request")),
            subject=data.get("subject", ""),
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            reply_to=data.get("reply_to"),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "sender": self.sender,
            "msg_type": self.type,
            "subject": self.subject,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "reply_to": self.reply_to,
        }


class AgentMeshClient:
    """Client for connecting to the Agent Mesh via NATS."""

    def __init__(
        self,
        agent_name: str,
        nats_url: str = "nats://100.101.32.70:4222",
        heartbeat_interval: int = 30,
    ):
        self.agent_name = agent_name
        self.nats_url = nats_url
        self.heartbeat_interval = heartbeat_interval
        self.nc: Optional[nats.NATS] = None
        self.subscriptions: list[Any] = []
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        self.nc = await nats.connect(self.nats_url, name=self.agent_name)
        logger.info("%s connected to NATS at %s", self.agent_name, self.nats_url)

    async def disconnect(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        for sub in self.subscriptions:
            await sub.unsubscribe()
        self.subscriptions.clear()
        if self.nc:
            await self.nc.drain()
            self.nc = None
            logger.info("%s disconnected from NATS", self.agent_name)

    async def subscribe(
        self,
        subject: str,
        callback: Callable[[MeshMessage], Any],
        queue: Optional[str] = None,
    ) -> None:
        if not self.nc:
            raise RuntimeError("Not connected to NATS")

        async def _handler(msg: Msg) -> None:
            try:
                raw = json.loads(msg.data.decode())
                mesh_msg = MeshMessage.from_json(raw)
                logger.debug("Received on %s: %s", msg.subject, mesh_msg.id)
                await callback(mesh_msg)
            except Exception as e:
                logger.error("Error handling message on %s: %s", msg.subject, e)

        sub = await self.nc.subscribe(subject, cb=_handler, queue=queue)
        self.subscriptions.append(sub)
        logger.info("Subscribed to %s", subject)

    async def publish(self, message: MeshMessage) -> None:
        if not self.nc:
            raise RuntimeError("Not connected to NATS")
        data = json.dumps(message.to_json()).encode()
        await self.nc.publish(message.subject, data)
        logger.info("Published %s to %s", message.id, message.subject)

    async def request(
        self, subject: str, payload: dict[str, Any], timeout: float = 10.0
    ) -> Optional[MeshMessage]:
        if not self.nc:
            raise RuntimeError("Not connected to NATS")
        msg = MeshMessage(
            id=str(uuid.uuid4()),
            sender=self.agent_name,
            type="request",
            subject=subject,
            payload=payload,
        )
        data = json.dumps(msg.to_json()).encode()
        try:
            response = await self.nc.request(subject, data, timeout=timeout)
            raw = json.loads(response.data.decode())
            return MeshMessage.from_json(raw)
        except nats.errors.TimeoutError:
            logger.warning("Request on %s timed out", subject)
            return None

    async def send_heartbeat(self) -> None:
        msg = MeshMessage(
            id=str(uuid.uuid4()),
            sender=self.agent_name,
            type="heartbeat",
            subject=f"agent.{self.agent_name}.heartbeat",
            payload={"agent": self.agent_name, "status": "alive"},
        )
        await self.publish(msg)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                await self.send_heartbeat()
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Heartbeat error: %s", e)
                await asyncio.sleep(self.heartbeat_interval)

    async def start_heartbeat(self) -> None:
        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("Heartbeat started (every %ds)", self.heartbeat_interval)
