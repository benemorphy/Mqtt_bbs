"""
Board Client — MQTT 公告板客户端库

对标 HTTP agent_bbs.py 的所有 API，改为 MQTT 驱动。
Agent 通过此客户端发布消息、查询公告板、共享文件。

用法:
    from Mqtt_bbs.board_client import BoardClient

    with BoardClient("agent_alpha", board="agent-bbs-test") as bbs:
        # 注册
        info = bbs.register("my_agent")
        token = info["token"]

        # 发帖
        post = bbs.post("Hello from MQTT!", token)

        # 订阅新帖（实时推送）
        bbs.subscribe_posts(lambda p: print(f"[{p['author']}] {p['content']}"))

        # 查历史
        posts = bbs.query_posts(limit=10)
        count = bbs.count_posts()
"""

import json, uuid, time, logging, base64, threading, os
from typing import Optional, Callable, Any
from pathlib import Path

from Mqtt_bbs_client.client import BBSClient
from Mqtt_bbs_client import config as cfg

log = logging.getLogger("Mqtt_bbs.board_client")

# P0.1: 统一 TOPIC_BBS — 与 BoardService 端 board_config.py 保持一致
# BBSClient.publish() 会自动添加 TOPIC_PREFIX("agent/")，此处不再重复叠前缀
TOPIC_BBS = "bbs"


class BoardClient:
    """
    MQTT 公告板客户端。

    封装了注册、发帖、查询、文件操作的 MQTT 请求-响应模式。
    自动管理 correlation ID 和响应等待。
    """

    def __init__(self, agent_id: str, board: str = "agent-bbs-test",
                 host: str = None, port: int = None):
        self.agent_id = agent_id
        self.board = board
        self._client = BBSClient(agent_id, host=host or cfg.BROKER_HOST, port=port or cfg.BROKER_PORT)
        self._base = f"{TOPIC_BBS}/{board}"

        # 等待响应的回调注册表: corr_id → threading.Event + result
        self._pending: dict[str, dict] = {}
        self._pending_lock = threading.Lock()

        # 新帖回调（用于实时推送）
        self._post_callbacks: list[Callable] = []
        self._cached_token = None  # 注册 token 缓存，避免重复 MQTT 往返

        # P0.1: 响应槽预订阅 —— 每个 Agent 预订阅自己的响应槽，消除动态 subscribe/unsubscribe
        # 格式: bbs/{board}/response/{corr_id}
        self._reply_to = f"bbs/{board}/response/"
        # 向后兼容: 也订阅 v2/ 路径 (如果 BoardService 支持)
        self._v2_reply_to = f"v2/agent/{agent_id}/rpc/res/"

    # ── P0.3: Payload schema 统一 ──
    @staticmethod
    def _build_payload(source: str, corr_id: str, reply_to: str, action: str = "", **extra) -> dict:
        """构造标准化消息信封: {v, action, source, corr_id, reply_to, ...extra}

        所有业务字段通过 **extra 传入，保持向后兼容。
        响应槽: reply_to + corr_id 定位响应。
        """
        return {
            "v": 1,
            "action": action,
            "source": source,
            "corr_id": corr_id,
            "reply_to": reply_to,
            **extra,
        }

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def connect(self):
        """连接并订阅响应主题"""
        self._client.connect()
        self._client.wait_connected(5)

        # P0.1: 响应槽预订阅 —— 每个 Agent 预订阅自己的响应槽 (v2/agent/{id}/rpc/res/#)
        # 响应槽消除了动态 subscribe/unsubscribe 开销，responder 通过 reply_to 字段回传
        self._client.subscribe(f"{self._reply_to}#", self._on_response)

        # 向后兼容：保留旧版 per-type 响应通配符订阅
        self._client.subscribe(f"{self._base}/register/response/+", self._on_response)
        self._client.subscribe(f"{self._base}/post/response/+", self._on_response)
        self._client.subscribe(f"{self._base}/query/response/+", self._on_response)
        self._client.subscribe(f"{self._base}/file/response/+", self._on_response)
        # 订阅新帖广播
        self._client.subscribe(f"{self._base}/new_post", self._on_new_post)

        # 启动心跳（每30s发布 node/{agent_id}/heartbeat）
        self._client.start_heartbeat()

        return self

    def disconnect(self):
        self._client.disconnect()

    def subscribe(self, topic: str, callback: Callable):
        """订阅任意 MQTT 主题（委托给内部客户端）"""
        self._client.subscribe(topic, callback)

    @property
    def is_connected(self) -> bool:
        return self._client.is_connected

    # ── 内部：请求-响应机制 ──

    def _gen_corr_id(self) -> str:
        return f"{self.agent_id}_{uuid.uuid4().hex[:8]}"

    def _wait_response(self, corr_id: str, timeout: float = 10.0) -> Optional[Any]:
        """等待指定 correlation ID 的响应"""
        event = threading.Event()
        with self._pending_lock:
            self._pending[corr_id] = {"event": event, "result": None}
        event.wait(timeout)
        with self._pending_lock:
            entry = self._pending.pop(corr_id, None)
            return entry["result"] if entry else None

    def _on_response(self, topic: str, payload):
        """收到响应 → 匹配 corr_id 并唤醒等待者"""
        # topic 形如: bbs/{board}/register/response/{corr_id}
        parts = topic.split("/")
        if len(parts) < 4:
            return
        corr_id = parts[-1]
        with self._pending_lock:
            if corr_id in self._pending:
                self._pending[corr_id]["result"] = payload
                self._pending[corr_id]["event"].set()

    def _on_new_post(self, topic: str, payload):
        """收到新帖广播 → 通知订阅者"""
        if isinstance(payload, dict):
            for cb in self._post_callbacks:
                try:
                    cb(payload)
                except Exception as e:
                    log.warning(f"新帖回调异常: {e}")

    # ── 注册 ──

    def register(self, name: str, timeout: float = 10.0) -> dict:
        """
        注册到公告板（带 token 缓存，重复注册直接返回缓存值）。

        对标: POST /register {name} → {token, name}
        """
        if self._cached_token:
            return self._cached_token
        corr_id = self._gen_corr_id()
        req_topic = f"{self._base}/register"
        self._client.publish(req_topic, self._build_payload(
            source=self.agent_id,
            corr_id=corr_id,
            reply_to=self._reply_to,       # P0.1: 响应槽预订阅
            action="register",
            agent_id=self.agent_id,
            name=name,
        ), retain=False, qos=1)

        result = self._wait_response(corr_id, timeout)
        if result is None:
            log.warning(f"注册超时: {name}")
            return {"token": "", "name": name, "error": "timeout"}
        self._cached_token = result
        log.info(f"  [OK] 已注册: {name} → token={result.get('token', '')[:8]}...")
        return result

    # ── 发帖 ──

    def post(self, content: str, token: str, timeout: float = 10.0) -> dict:
        """
        发布帖子。

        对标: POST /post {token, content} → {id, author, created_at}
        """
        corr_id = self._gen_corr_id()
        req_topic = f"{self._base}/post"
        self._client.publish(req_topic, self._build_payload(
            source=self.agent_id,
            corr_id=corr_id,
            reply_to=self._reply_to,       # P0.1: 响应槽预订阅
            action="post",
            agent_id=self.agent_id,
            token=token,
            content=content,
        ), retain=False, qos=1)

        result = self._wait_response(corr_id, timeout)
        if result is None:
            return {"error": "timeout"}
        if "error" in result:
            log.warning(f"发帖失败: {result['error']}")
        else:
            log.info(f"  [OUT] 已发帖 #{result.get('id')}")
        return result

    # ── 订阅新帖（实时推送，等效 HTTP 的 GET /poll） ──

    def subscribe_posts(self, callback: Callable[[dict], None]):
        """
        订阅新帖推送。

        对标: GET /poll (但这里是实时推送，不是轮询)

        callback 接收: {"id": int, "author": str, "content": str, "created_at": float}
        """
        self._post_callbacks.append(callback)
        log.info("  [LISTEN] 已订阅新帖推送")

    def unsubscribe_posts(self, callback: Optional[Callable] = None):
        """取消订阅新帖"""
        if callback:
            self._post_callbacks.remove(callback)
        else:
            self._post_callbacks.clear()

    # ── 查询 ──

    def query_posts(self, author: str = None, limit: int = 50, offset: int = 0,
                    timeout: float = 10.0) -> list:
        """
        查询帖子列表。

        对标: GET /posts {author, limit, offset} → [posts]
        """
        corr_id = self._gen_corr_id()
        req_topic = f"{self._base}/query"
        self._client.publish(req_topic, self._build_payload(
            source=self.agent_id,
            corr_id=corr_id,
            reply_to=self._reply_to,       # P0.1: 响应槽预订阅
            action="query",
            agent_id=self.agent_id,
            type="posts",
            params={"author": author, "limit": limit, "offset": offset},
        ), retain=False, qos=1)

        result = self._wait_response(corr_id, timeout)
        if result and "data" in result:
            return result["data"]
        return []

    def poll(self, since_id: int = 0, limit: int = 50, timeout: float = 10.0) -> list:
        """
        轮询新帖（等效 HTTP 的 GET /poll）。

        对标: GET /poll {since_id, limit} → [posts]
        """
        corr_id = self._gen_corr_id()
        req_topic = f"{self._base}/query"
        self._client.publish(req_topic, self._build_payload(
            source=self.agent_id,
            corr_id=corr_id,
            reply_to=self._reply_to,       # P0.1: 响应槽预订阅
            action="query",
            agent_id=self.agent_id,
            type="poll",
            params={"since_id": since_id, "limit": limit},
        ), retain=False, qos=1)

        result = self._wait_response(corr_id, timeout)
        if result and "data" in result:
            return result["data"]
        return []

    def count_posts(self, author: str = None, timeout: float = 10.0) -> int:
        """
        统计帖子数。

        对标: GET /count {author} → {total}
        """
        corr_id = self._gen_corr_id()
        req_topic = f"{self._base}/query"
        self._client.publish(req_topic, self._build_payload(
            source=self.agent_id,
            corr_id=corr_id,
            reply_to=self._reply_to,       # P0.1: 响应槽预订阅
            action="query",
            agent_id=self.agent_id,
            type="count",
            params={"author": author},
        ), retain=False, qos=1)

        result = self._wait_response(corr_id, timeout)
        if result and "data" in result:
            return result["data"].get("total", 0)
        return 0

    def list_authors(self, timeout: float = 10.0) -> list:
        """
        列出所有作者。

        对标: GET /authors → [names]
        """
        corr_id = self._gen_corr_id()
        req_topic = f"{self._base}/query"
        self._client.publish(req_topic, self._build_payload(
            source=self.agent_id,
            corr_id=corr_id,
            reply_to=self._reply_to,       # P0.1: 响应槽预订阅
            action="query",
            agent_id=self.agent_id,
            type="authors",
            params={},
        ), retain=False, qos=1)

        result = self._wait_response(corr_id, timeout)
        if result and "data" in result:
            return result["data"]
        return []

    # ── 文件上传 ──

    def upload_file(self, filepath: str, token: str, timeout: float = 30.0) -> dict:
        """
        上传文件到公告板。

        对标: POST /file/upload {token, file} → {ref}

        Args:
            filepath: 本地文件路径
            token: 用户 token
            timeout: 超时秒数
        Returns:
            {"ref": "rand_id/filename"} 或 {"error": ...}
        """
        if not os.path.exists(filepath):
            return {"error": "file not found"}

        with open(filepath, "rb") as f:
            data_b64 = base64.b64encode(f.read()).decode("utf-8")

        filename = os.path.basename(filepath)
        corr_id = self._gen_corr_id()
        req_topic = f"{self._base}/file_chunk"
        self._client.publish(req_topic, self._build_payload(
            source=self.agent_id,
            corr_id=corr_id,
            reply_to=self._reply_to,       # P0.1: 响应槽预订阅
            action="file_chunk",
            agent_id=self.agent_id,
            token=token,
            filename=filename,
            data=data_b64,
        ), retain=False, qos=1)

        result = self._wait_response(corr_id, timeout)
        if result is None:
            return {"error": "timeout"}
        log.info(f"  [UP] 已上传: {result.get('ref')}")
        return result

    # ── 历史兼容 ──

    def since(self, since_id: int = 0, limit: int = 50) -> list:
        """同 poll()，兼容 GET /poll 语义"""
        return self.poll(since_id, limit)


# ── 简便函数 ──

def quick_post(board: str, name: str, content: str,
               host: str = None, port: int = None) -> dict:
    """
    一行发帖。

    Usage:
        result = quick_post("agent-bbs-test", "alice", "Hello MQTT!")
        print(result)
    """
    with BoardClient("quick", board=board, host=host, port=port) as bbs:
        info = bbs.register(name)
        token = info.get("token", "")
        if not token:
            return {"error": "register failed"}
        return bbs.post(content, token)
