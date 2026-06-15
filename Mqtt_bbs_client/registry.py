"""
RetainCapabilityRegistry — 无状态化 CapabilityRegistry

利用 MQTT Retain 消息替代进程内 _agents dict。
多 BoardService 实例看到相同的 retain 消息，无需进程间同步。
重启后 1s 内自动重建，无需 Agent 重新心跳。

用法:
    registry = RetainCapabilityRegistry(client)
    registry.start()
    agents = registry.get_agents()
"""

import time, threading, logging
from typing import Optional

log = logging.getLogger("Mqtt_bbs.registry")


class RetainCapabilityRegistry:
    """
    利用 MQTT Retain 消息驱动的 Agent 能力注册表（无状态版）。

    设计原则：
    - 不维护进程内 _agents dict → 多实例一致
    - 不依赖 cleanup 循环 → 依赖 LWT 实时离线通知
    - 所有查询通过收集 retain 消息完成
    """

    def __init__(self, client):
        self._client = client
        self._running = False
        # 短 TTL 缓存（秒），避免每次查询都收集 retain
        self._cache_ttl = 1.0
        self._cache = {}  # agent_id → agent_info
        self._cache_time = 0.0
        self._cache_lock = threading.Lock()

    # ── 生命周期 ──

    def start(self):
        """启动注册表：订阅能力/心跳/状态/查询主题"""
        self._running = True
        self._client.subscribe("node/+/capability", self._on_capability)
        self._client.subscribe("node/+/heartbeat", self._on_heartbeat)
        self._client.subscribe("node/+/status", self._on_status)
        self._client.subscribe("board/capability/query", self._on_query)
        log.info("[RetainRegistry] 启动")

    def stop(self):
        self._running = False

    # ── 公开 API ──

    def get_agents(self, capability: Optional[str] = None) -> list[dict]:
        """获取注册的 Agent 列表，可选按能力过滤"""
        agents = self._collect()
        if capability:
            agents = [a for a in agents if capability in a.get("capabilities", [])]
        return agents

    def get_agent(self, agent_id: str) -> Optional[dict]:
        """获取单个 Agent 信息"""
        agents = self._collect()
        for a in agents:
            if a.get("agent_id") == agent_id:
                return a
        return None

    # ── 内部：收集 retain 消息 ──

    def _collect(self) -> list[dict]:
        """
        收集所有 Agent 状态（缓存优先，最多 1s 过期）。

        缓存命中 → 直接返回
        缓存过期 → passive 收集（监听中已有 retain，不需要额外订阅）
        """
        now = time.time()
        with self._cache_lock:
            if now - self._cache_time < self._cache_ttl and self._cache:
                return list(self._cache.values())

        # 缓存过期 → 主动收集 retain 消息
        return self._collect_retain()

    def _collect_retain(self) -> list[dict]:
        """
        通过临时订阅 + 超时收集所有 retain 消息。

        收集 node/+/capability（能力声明）和 node/+/status（状态）
        合并为统一的 agent 信息列表。
        """
        collected = {}
        event = threading.Event()
        results = {}

        def on_capability(topic, payload):
            parts = topic.split("/")
            if len(parts) < 3:
                return
            aid = parts[1]
            caps = payload.get("capabilities", []) if isinstance(payload, dict) else []
            if aid not in results:
                results[aid] = {"agent_id": aid, "capabilities": [], "status": "unknown", "last_seen": 0}
            results[aid]["capabilities"] = caps
            results[aid]["last_seen"] = time.time()

        def on_status(topic, payload):
            parts = topic.split("/")
            if len(parts) < 3:
                return
            aid = parts[1]
            status = "online"
            if isinstance(payload, bytes):
                status = payload.decode("utf-8")
            elif isinstance(payload, str):
                status = payload
            if aid not in results:
                results[aid] = {"agent_id": aid, "capabilities": [], "status": status, "last_seen": 0}
            else:
                results[aid]["status"] = status
            if status == "online":
                results[aid]["last_seen"] = time.time()

        # 临时订阅
        sub1 = self._client.subscribe("node/+/capability", on_capability)
        sub2 = self._client.subscribe("node/+/status", on_status)

        # 等待 retain 推送（retain 消息在订阅后立即发送）
        time.sleep(0.5)

        self._client.unsubscribe(sub1)
        self._client.unsubscribe(sub2)

        # 更新缓存
        with self._cache_lock:
            self._cache = results
            self._cache_time = time.time()

        return list(results.values())

    # ── 内部：消息处理器（维持 TTL 缓存） ──

    def _on_capability(self, topic: str, payload):
        """处理能力声明（retain）"""
        parts = topic.split("/")
        if len(parts) < 3:
            return
        aid = parts[1]
        caps = payload.get("capabilities", []) if isinstance(payload, dict) else []
        with self._cache_lock:
            if aid not in self._cache:
                self._cache[aid] = {"agent_id": aid, "capabilities": [], "status": "unknown", "last_seen": 0}
            self._cache[aid]["capabilities"] = caps
            self._cache[aid]["last_seen"] = time.time()
        log.info(f"  [RetainRegistry] 能力声明: {aid} -> {caps}")

    def _on_heartbeat(self, topic: str, payload):
        """处理心跳"""
        parts = topic.split("/")
        if len(parts) < 3:
            return
        aid = parts[1]
        with self._cache_lock:
            if aid not in self._cache:
                self._cache[aid] = {"agent_id": aid, "capabilities": [], "status": "online", "last_seen": time.time()}
            else:
                self._cache[aid]["status"] = "online"
                self._cache[aid]["last_seen"] = time.time()
        # 不打印心跳日志避免刷屏

    def _on_status(self, topic: str, payload):
        """处理状态变更（含 LWT）"""
        parts = topic.split("/")
        if len(parts) < 3:
            return
        aid = parts[1]
        status = "online"
        if isinstance(payload, bytes):
            status = payload.decode("utf-8")
        elif isinstance(payload, str):
            status = payload
        with self._cache_lock:
            if aid not in self._cache:
                self._cache[aid] = {"agent_id": aid, "capabilities": [], "status": status, "last_seen": 0}
            else:
                self._cache[aid]["status"] = status
                if status == "online":
                    self._cache[aid]["last_seen"] = time.time()
        log.info(f"  [RetainRegistry] 状态变更: {aid} -> {status}")

    def _on_query(self, topic: str, payload):
        """处理能力查询请求"""
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
        log.info(f"  [RetainRegistry] 查询: filter={capability_filter} -> {len(agents)} agents")
