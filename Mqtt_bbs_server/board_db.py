"""
Board Service — 数据库与能力注册表模块

从 board_service.py 拆分而来。
包含 CapabilityRegistry（MQTT 驱动的 Agent 能力注册表）
和 _MariaDBWrapper（SQLite 兼容的 MariaDB 封装）。
"""

import json
import time
import logging
import threading
from typing import Optional

from Mqtt_bbs_client.client import BBSClient
from .board_config import log


class CapabilityRegistry:
    """
    MQTT-driven Agent Capability Registry.

    Subscribes to:
      - node/{agent_id}/capability (retained) for capability declarations
      - node/{agent_id}/heartbeat for liveness
      - node/{agent_id}/status (LWT) for offline detection
      - board/capability/query for queries
    """

    def __init__(self, client: BBSClient):
        self._client = client
        self._lock = threading.Lock()
        self._agents: dict[str, dict] = {}
        self._running = False

    def start(self):
        """Start registry: subscribe to relevant topics"""
        self._running = True
        self._client.subscribe("node/+/capability", self._on_capability)
        self._client.subscribe("node/+/heartbeat", self._on_heartbeat)
        self._client.subscribe("node/+/status", self._on_status)
        self._client.subscribe("board/capability/query", self._on_query)
        log.info("[CapabilityRegistry] started")

    def stop(self):
        self._running = False

    def get_agents(self, capability: Optional[str] = None) -> list[dict]:
        """Get registered agents, optionally filtered by capability"""
        with self._lock:
            agents = list(self._agents.values())
        if capability:
            agents = [a for a in agents if capability in a.get("capabilities", [])]
        return agents

    def get_agent(self, agent_id: str) -> Optional[dict]:
        with self._lock:
            return self._agents.get(agent_id)

    def _on_capability(self, topic: str, payload):
        parts = topic.split("/")
        if len(parts) < 3:
            return
        agent_id = parts[1]
        caps = []
        if isinstance(payload, dict):
            caps = payload.get("capabilities", [])
        with self._lock:
            if agent_id not in self._agents:
                self._agents[agent_id] = {
                    "agent_id": agent_id, "capabilities": [],
                    "status": "unknown", "last_seen": time.time()
                }
            self._agents[agent_id]["capabilities"] = caps
            self._agents[agent_id]["last_seen"] = time.time()
        log.info(f"  [TASK] capability: {agent_id} -> {caps}")

    def _on_heartbeat(self, topic: str, payload):
        parts = topic.split("/")
        if len(parts) < 3:
            return
        agent_id = parts[1]
        caps = []
        load = None
        if isinstance(payload, dict):
            caps = payload.get("capabilities", [])
            load = payload.get("load")
        with self._lock:
            if agent_id not in self._agents:
                self._agents[agent_id] = {
                    "agent_id": agent_id, "capabilities": [],
                    "status": "online", "last_seen": time.time()
                }
            if caps:
                self._agents[agent_id]["capabilities"] = caps
            self._agents[agent_id]["status"] = "online"
            self._agents[agent_id]["last_seen"] = time.time()
            if load is not None:
                self._agents[agent_id]["load"] = load

    def _on_status(self, topic: str, payload):
        parts = topic.split("/")
        if len(parts) < 3:
            return
        agent_id = parts[1]
        status = "online"
        if isinstance(payload, bytes):
            status = payload.decode("utf-8")
        elif isinstance(payload, str):
            status = payload
        with self._lock:
            if agent_id not in self._agents:
                self._agents[agent_id] = {
                    "agent_id": agent_id, "capabilities": [],
                    "status": status, "last_seen": time.time()
                }
            else:
                self._agents[agent_id]["status"] = status
                if status == "online":
                    self._agents[agent_id]["last_seen"] = time.time()
        log.info(f"  [INFO] status: {agent_id} -> {status}")

    def _on_query(self, topic: str, payload):
        corr_id = ""
        capability_filter = None
        if isinstance(payload, dict):
            corr_id = payload.get("corr_id", "")
            capability_filter = payload.get("capability")
        agents = self.get_agents(capability_filter)
        resp = {
            "type": "capability_list",
            "agents": agents,
            "count": len(agents),
            "timestamp": time.time(),
        }
        if corr_id:
            self._client.publish(f"board/capability/query/response/{corr_id}", resp, retain=False)
        else:
            self._client.publish("board/capability/query/response", resp, retain=False)
        log.info(f"  query: filter={capability_filter} -> {len(agents)} agents")


class MariaDBWrapper:
    """Wrap pymysql.Connection with SQLite-compatible .execute() shortcut"""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def cursor(self):
        return self._conn.cursor()

    def close(self):
        self._conn.close()
