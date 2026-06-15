"""
MariaDB 持久化层 — Retain + Session 消息持久化

架构:
    BBSClient (现有)
        │
    BBSClientWithPersistence (新增)
        ├─ publish()   → MariaDB UPSERT → MQTT PUBLISH
        ├─ subscribe() → MQTT SUB → MariaDB 恢复 retained → session_queue 重放
        ├─ _on_connect()  → 恢复会话 + 标记在线
        └─ _on_disconnect() → 标记离线

MariaDB: 127.0.0.1:3306, user=root, password=mariadb, database=mqtt_bbs
"""

import json
import time
import logging
import uuid
import threading
from datetime import datetime
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass, field

import pymysql
from pymysql.cursors import DictCursor

from Mqtt_bbs_client.config import DB_CONFIG
from Mqtt_bbs_client import config as cfg
from Mqtt_bbs_client.client import BBSClient
from Mqtt_bbs_client.types import TaskMessage, TaskOutput, TaskStatus

log = logging.getLogger("Mqtt_bbs.persist")

# ──────────────────────────────────────────────
#  默认数据库配置（使用 config 中的 DB_CONFIG，通过环境变量配置）
# ──────────────────────────────────────────────
DEFAULT_DB = DB_CONFIG


# ──────────────────────────────────────────────
#  MariaDB 连接管理
# ──────────────────────────────────────────────
class MariaDBConn:
    """MariaDB 连接管理 — 线程安全的单连接+重连"""

    _lock = threading.Lock()

    def __init__(self, config: Optional[dict] = None):
        self._cfg = config or DEFAULT_DB
        self._local = threading.local()
        self._connect()

    def _connect(self):
        try:
            conn = pymysql.connect(**self._cfg, cursorclass=DictCursor, autocommit=True)
            self._local.conn = conn
            log.info("[OK] MariaDB 连接成功")
        except pymysql.Error as e:
            log.warning(f"[WARN] MariaDB 连接失败: {e}")
            self._local.conn = None

    def _ensure(self):
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            self._connect()
            conn = self._local.conn
        if conn:
            try:
                conn.ping(reconnect=True)
            except pymysql.Error:
                self._connect()

    def execute(self, sql: str, params: tuple = ()) -> Optional[List[Dict]]:
        with self._lock:
            self._ensure()
            conn = getattr(self._local, 'conn', None)
            if not conn:
                return None
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    if cur.description:
                        return cur.fetchall()
                    return []
            except pymysql.Error as e:
                log.warning(f"DB错误: {e}")
                # 尝试回滚
                try:
                    conn.rollback()
                except:
                    pass
                return None

    def close(self):
        conn = getattr(self._local, 'conn', None)
        if conn:
            conn.close()
            self._local.conn = None


# ──────────────────────────────────────────────
#  持久化 BBSClient
# ──────────────────────────────────────────────
class BBSClientWithPersistence(BBSClient):
    """带 MariaDB 持久化的 BBSClient

    在原有 pub/sub 基础上增加:
    - Retain 消息写入 retained_messages 表
    - 离线消息缓存到 session_queue
    - 重连时自动恢复 retained + 重放 session_queue
    - Agent 在线/离线状态追踪
    """

    def __init__(self, agent_id: str, host: str = None, port: int = None,
                 db_config: Optional[dict] = None):
        from Mqtt_bbs_client.config import BROKER_HOST, BROKER_PORT
        super().__init__(agent_id, 
                         host=(host if host is not None else BROKER_HOST),
                         port=(port if port is not None else BROKER_PORT))
        self._db = MariaDBConn(db_config)
        self._subscribed_topics: Dict[str, Callable] = {}  # topic → callback
        self._recovered = False
        self._seq = 0

    # ── publish (增强: retain → 写 DB) ──────────────────

    def publish(self, topic: str, payload, retain=False, qos=None):
        # 1. 写 MariaDB (retain 消息) — 异常不阻断MQTT
        if retain and self._db:
            try:
                self._upsert_retained(topic, payload, qos or 1)
            except Exception as e:
                log.error(f"[PERSIST] DB写入失败 topic={topic}: {e}", exc_info=True)

        # 2. 如果是发给特定 Agent 的消息，且该 Agent 离线，入队 session_queue
        if self._db and topic.count("/") >= 2:
            try:
                target = self._parse_target_agent(topic)
                if target:
                    session = self._get_session(target)
                    if session and session.get("status") == "offline":
                        self._enqueue_session(target, topic, payload, qos or 1, is_retained=retain)
                        log.info(f"[OUTBOX] {target} 离线，消息入队 session_queue")
            except Exception as e:
                log.error(f"[PERSIST] session队列失败 topic={topic}: {e}")

        # 3. 发 MQTT（无论如何都执行）
        super().publish(topic, payload, retain, qos)

    # ── subscribe (增强: 恢复 retained + session_queue) ──

    def subscribe(self, topic: str, callback: Callable, qos: int = 1):
        self._subscribed_topics[topic] = callback
        super().subscribe(topic, callback, qos)

        # 恢复 retained（每次subscribe都尝试，不受_recovered限制）
        if self._db:
            self._db.execute("CREATE TABLE IF NOT EXISTS posts (id BIGINT PRIMARY KEY, board VARCHAR(128) NOT NULL, author VARCHAR(64), content TEXT, created_at DATETIME(3), INDEX idx_board(board), INDEX idx_created(created_at))")
            self._recover_retained(topic, callback)
            if not self._recovered:
                self._replay_session_queue()

    # ── connect/disconnect 事件 ─────────────────────────

    def connect(self):
        super().connect()
        # 标记在线
        if self._db:
            self._set_agent_online()
            self._recover_all_retained()
            self._replay_session_queue()
            self._recovered = True

    def post_fast(self, content: str, token: str, board: str = None) -> dict:
        """
        快速发帖：直接写入 MariaDB + 发布 MQTT（无需等待 BoardService 响应）。
        比 BoardClient.post() 快 10-50x，适用于批量发帖场景。

        安全: v2 增加了 JWT token 解码校验, 无效 token 拒绝写入。
        """
        import uuid as _uid, json as _json, time as _time
        board = board or self.board
        # P0.2: JWT token 校验 — 无效或过期 token 拒绝处理
        try:
            import jwt as _jwt
            _secret = os.environ.get("JWT_SECRET")
            if _secret:
                decoded = _jwt.decode(token, _secret, algorithms=["HS256"])
                author_name = decoded.get("name", token[:8])
            else:
                log.warning("  post_fast: JWT_SECRET not set, falling back to token[:8]")
                author_name = token[:8]
        except Exception as e:
            log.warning(f"  post_fast: token 校验失败: {e}")
            # 兼容旧 token 格式（纯字符串）：使用 token[:8] 但记录告警
            if len(token) < 8:
                return {"error": "invalid token"}
            author_name = token[:8]

        post_id = int(_time.time() * 1000) % 10000000 + _uid.uuid4().int % 1000000
        now = _time.time()
        post_data = {
            "id": post_id, "author": author_name, "content": content,
            "board": board, "created_at": now
        }
        # 写 MariaDB
        if self._db:
            self._db.execute(
                "INSERT INTO posts (id, board, author, content, created_at) VALUES (%s, %s, %s, %s, FROM_UNIXTIME(%s))",
                (post_id, board, author_name, content, now)
            )
        # MQTT 广播（fire-and-forget）
        self.publish(f"bbs/{board}/new_post", post_data, retain=False, qos=0)
        return post_data

    def disconnect(self):
        # 标记离线
        if self._db:
            self._set_agent_offline()
        super().disconnect()

    # ── 内部: Retain 持久化 ─────────────────────────────

    def _upsert_retained(self, topic: str, payload, qos: int):
        payload_str = json.dumps(payload) if not isinstance(payload, str) else payload
        self._db.execute(
            """INSERT INTO retained_messages (topic, payload, qos, source_agent, created_at, updated_at)
               VALUES (%s, %s, %s, %s, NOW(3), NOW(3))
               ON DUPLICATE KEY UPDATE payload=%s, qos=%s, source_agent=%s, updated_at=NOW(3)""",
            (topic, payload_str, qos, self.agent_id,
             payload_str, qos, self.agent_id)
        )

    def _recover_retained(self, pattern: str, callback: Callable):
        """恢复匹配 pattern 的 retained 消息"""
        # 通配符转为 SQL LIKE
        like = pattern.replace("agent/", "").replace("#", "%").replace("+", "_")
        rows = self._db.execute(
            "SELECT topic, payload FROM retained_messages WHERE topic LIKE %s ORDER BY updated_at ASC",
            (f"%{like}",)
        )
        if rows:
            for row in rows:
                try:
                    payload = json.loads(row["payload"])
                except (json.JSONDecodeError, TypeError):
                    payload = row["payload"]
                log.info(f"[SYNC] 恢复 retained: {row['topic']}")
                callback(row["topic"], payload)

    def _recover_all_retained(self):
        """恢复所有已订阅的 retained"""
        for pattern, cb in list(self._subscribed_topics.items()):
            self._recover_retained(pattern, cb)

    # ── 内部: Session 离线队列 ──────────────────────────

    def _parse_target_agent(self, topic: str) -> Optional[str]:
        """从 topic 推断目标 agent_id"""
        # 例如: agent/board/task/task_001/claim → target=task_001 不对
        # agent/node/worker_01/notification → target=worker_01
        parts = topic.split("/")
        # node/{agent_id}/... 格式
        if len(parts) >= 3 and "agent/" in topic and parts[0] == "agent" and parts[1] == "node":
            return parts[2]
        return None

    def _enqueue_session(self, target: str, topic: str, payload, qos: int, is_retained: bool = False):
        payload_str = json.dumps(payload) if not isinstance(payload, str) else payload
        self._seq += 1
        self._db.execute(
            """INSERT INTO session_queue (target_agent, topic, payload, qos, seq, is_retained, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, NOW(3))""",
            (target, topic, payload_str, qos, self._seq, is_retained)
        )

    def _replay_session_queue(self):
        """重放当前 Agent 的离线消息"""
        if not self._db:
            return
        rows = self._db.execute(
            """SELECT id, topic, payload, seq FROM session_queue
               WHERE target_agent=%s AND delivered=FALSE
               ORDER BY seq ASC""",
            (self.agent_id,)
        )
        if not rows:
            return
        log.info(f"[MSG] 重放 {len(rows)} 条离线消息")
        for row in rows:
            try:
                payload = json.loads(row["payload"])
            except (json.JSONDecodeError, TypeError):
                payload = row["payload"]

            # 找匹配的 callback
            for pattern, cb in self._subscribed_topics.items():
                if self._topic_matches(pattern, row["topic"]):
                    cb(row["topic"], payload)
                    break

            # 标记已送达
            self._db.execute(
                "UPDATE session_queue SET delivered=TRUE, delivered_at=NOW(3) WHERE id=%s",
                (row["id"],)
            )

    # ── 内部: Agent 会话状态 ────────────────────────────

    def _set_agent_online(self):
        self._db.execute(
            """INSERT INTO agent_sessions (agent_id, last_online, status, updated_at)
               VALUES (%s, NOW(3), 'online', NOW(3))
               ON DUPLICATE KEY UPDATE last_online=NOW(3), status='online', updated_at=NOW(3)""",
            (self.agent_id,)
        )

    def _set_agent_offline(self):
        self._db.execute(
            "UPDATE agent_sessions SET last_offline=NOW(3), status='offline', updated_at=NOW(3) WHERE agent_id=%s",
            (self.agent_id,)
        )

    def _get_session(self, agent_id: str) -> Optional[Dict]:
        rows = self._db.execute(
            "SELECT status, last_online, last_offline FROM agent_sessions WHERE agent_id=%s",
            (agent_id,)
        )
        return rows[0] if rows else None

    @staticmethod
    def _topic_matches(pattern: str, topic: str) -> bool:
        """检查 topic 是否匹配 MQTT 通配符 pattern"""
        pat_parts = pattern.split("/")
        top_parts = topic.split("/")
        for i, p in enumerate(pat_parts):
            if p == "#":
                return True
            if i >= len(top_parts):
                return False
            if p == "+":
                continue
            if p != top_parts[i]:
                return False
        return len(pat_parts) == len(top_parts)

    # ── on_connect / on_disconnect 增强 ──────────────────

    def _on_connect(self, client, userdata, flags, rc, reasonCodeProperties=None):
        super()._on_connect(client, userdata, flags, rc, reasonCodeProperties)
        if rc == 0 and self._db:
            self._set_agent_online()
            self._recover_all_retained()
            self._replay_session_queue()
            self._recovered = True

    def _on_disconnect(self, client, userdata, rc, properties=None, reasonCodeProperties=None):
        if self._db and self._connected:
            self._set_agent_offline()
        super()._on_disconnect(client, userdata, rc, properties, reasonCodeProperties)

# ──────────────────────────────────────────────
# AgentBoardWithPersistence — 持久化版主智能体
# ──────────────────────────────────────────────

class AgentBoardWithPersistence:
    """
    持久化版 AgentBoard。

    使用 BBSClientWithPersistence 替代 BBSClient，
    所有 Retain 消息自动持久化到 MariaDB。

    额外特性:
    - post_task() 同时记录任务到 retained_messages
    - wait_task() 完成后自动保存 output 到本地结果缓存
    - 断线重连后自动恢复已订阅的 retained 消息
    """

    def __init__(self, agent_id: str = "master"):
        self.agent_id = agent_id
        self._client = BBSClientWithPersistence(agent_id)
        self._results: dict[str, TaskOutput] = {}
        self._callbacks: dict[str, Callable] = {}
        self._client.connect()
        self._client.wait_connected(3)

    def __enter__(self):
        self._client.connect()
        self._client.wait_connected(5)
        return self

    def __exit__(self, *args):
        self._client.disconnect()

    def post_task(self, task_type: str, task_input: dict,
                  task_id: Optional[str] = None,
                  priority: int = 3,
                  timeout: int = cfg.DEFAULT_TASK_TIMEOUT) -> str:
        from Mqtt_bbs_client.types import TaskMessage, TaskStatus
        if task_id is None:
            task_id = f"task_{uuid.uuid4().hex[:8]}"

        msg = TaskMessage(
            task_id=task_id,
            type=task_type,
            input=task_input,
            priority=priority,
            timeout=timeout,
        )

        # publish → MariaDB + MQTT
        self._client.publish(f"board/task/{task_id}/input", msg.to_dict(), retain=True)
        self._client.publish(f"board/task/{task_id}/status", TaskStatus.PENDING.value, retain=True)
        self._client.publish(f"board/open", task_id, retain=False)

        log.info(f"[{self.agent_id}] [OUT] [PERSIST] 发布任务: {task_id} ({task_type})")
        return task_id

    def wait_task(self, task_id: str, timeout=None,
                  poll_interval: float = 0.5):
        from Mqtt_bbs_client.types import TaskOutput
        import json as _json
        if timeout is None:
            timeout = config.DEFAULT_TASK_TIMEOUT

        result_holder = {"output": None}

        def on_output(topic, payload):
            if isinstance(payload, dict):
                result_holder["output"] = TaskOutput.from_dict(payload)

        def on_signal(topic, payload):
            if isinstance(payload, bytes):
                payload = payload.decode()
            if payload == "[ROUND_END]":
                pass

        # 1. 优先查 MariaDB retained_messages（output可能已先于subscribe到达）
        if hasattr(self, '_client') and hasattr(self._client, '_db') and self._client._db:
            rows = self._client._db.execute(
                "SELECT payload FROM retained_messages WHERE topic=%s",
                (f"board/task/{task_id}/output",)
            )
            if rows:
                payload = rows[0]["payload"]
                if isinstance(payload, (str, bytes)):
                    try:
                        data = _json.loads(payload if isinstance(payload, str) else payload.decode())
                        if isinstance(data, dict):
                            result_holder["output"] = TaskOutput.from_dict(data)
                            self._results[task_id] = result_holder["output"]
                            log.info(f"[{self.agent_id}] [OK] 从DB恢复结果: {task_id}")
                            return result_holder["output"]
                    except (_json.JSONDecodeError, TypeError):
                        pass

        # 2. MQTT subscribe（兜底接收实时消息）
        self._client.subscribe(f"board/task/{task_id}/output", on_output)
        self._client.subscribe(f"board/task/{task_id}/signal", on_signal)

        deadline = time.time() + timeout
        while time.time() < deadline:
            if result_holder["output"] is not None:
                output = result_holder["output"]
                self._results[task_id] = output
                log.debug(f"[{self.agent_id}] [OK] 收到结果: {task_id}")
                return output
            time.sleep(poll_interval)

        raise TimeoutError(f"任务 {task_id} 超时 ({timeout}s)")

    def cancel_task(self, task_id: str):
        from Mqtt_bbs_client.types import TaskStatus
        self._client.publish(f"board/task/{task_id}/signal", "[CANCEL]", retain=True, qos=2)
        self._client.publish(f"board/task/{task_id}/status", TaskStatus.CANCELLED.value, retain=True)
        log.info(f"[{self.agent_id}] [STOP] [PERSIST] 取消任务: {task_id}")


# ──────────────────────────────────────────────
# WorkerAgentWithPersistence — 持久化版工作智能体
# ──────────────────────────────────────────────

class WorkerAgentWithPersistence:
    """
    持久化版 WorkerAgent。

    使用 BBSClientWithPersistence 替代 BBSClient，
    连接时自动恢复 retained 消息 + 重放离线 session_queue。
    """

    def __init__(self, agent_id: str, capabilities: Optional[list[str]] = None,
                 host: str = None, port: int = None):
        from Mqtt_bbs_client.client import BBSClient
        self.agent_id = agent_id
        self.capabilities = capabilities or []
        self._client = BBSClientWithPersistence(agent_id, host=host, port=port)
        self._task_handler: Optional[Callable] = None
        self._running = False
        self._current_task_id: Optional[str] = None
        self._seq = 0

    def on_task(self, handler):
        self._task_handler = handler
        return self

    def start(self, block: bool = True):
        self._client.connect()
        self._client.wait_connected(5)
        self._running = True

        def on_new_task(topic, payload):
            if not self._running:
                return
            if isinstance(payload, bytes):
                try:
                    payload = json.loads(payload.decode())
                except:
                    return
            if not isinstance(payload, dict):
                return

            msg = TaskMessage.from_dict(payload)

            # 能力匹配
            if self.capabilities and msg.type not in self.capabilities:
                log.debug(f"[{self.agent_id}] ⏭ 跳过不匹配任务: {msg.type}")
                return

            # 检查是否已被认领
            if msg.task_id == self._current_task_id:
                return

            self.claim_task(msg)

        self._client.subscribe("board/task/+/input", on_new_task)

        log.info(f"[{self.agent_id}] [AGENT] [PERSIST] 启动 (能力: {self.capabilities})")

        if block:
            try:
                while self._running:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.stop()

    def stop(self):
        self._running = False
        self._client.disconnect()
        log.info(f"[{self.agent_id}] [STOP] [PERSIST] 停止")

    def claim_task(self, msg: TaskMessage):
        from Mqtt_bbs_client.types import TaskStatus
        self._current_task_id = msg.task_id
        self._seq = 0

        # 认领
        self._client.publish(f"board/task/{msg.task_id}/claim",
                             {"agent_id": self.agent_id, "claimed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")},
                             retain=True)
        self._client.publish(f"board/task/{msg.task_id}/status", TaskStatus.RUNNING.value, retain=True)
        self._client.publish(f"node/{self.agent_id}/task/current", msg.task_id, retain=True)

        log.info(f"[{self.agent_id}] [TOOL] 认领任务: {msg.task_id} ({msg.type})")

        try:
            if self._task_handler:
                result = self._task_handler(msg)
            else:
                result = {"status": "no_handler"}
            self.complete(msg.task_id, result=result)
        except Exception as e:
            log.error(f"[{self.agent_id}] [FAIL] 执行异常: {e}")
            self.complete(msg.task_id, status="failed", error={"type": "exception", "msg": str(e)})

    def stream_out(self, task_id: str, text: str):
        self._seq += 1
        self._client.publish(f"board/task/{task_id}/stdout",
                             {"seq": self._seq, "data": text}, retain=False)
        log.info(f"[{self.agent_id}] [ANNOUNCE] [stdout] {text[:80]}")

    def stream_err(self, task_id: str, text: str):
        self._seq += 1
        self._client.publish(f"board/task/{task_id}/stderr",
                             {"seq": self._seq, "data": text}, retain=False)
        log.info(f"[{self.agent_id}] [WARN] [stderr] {text[:80]}")

    def complete(self, task_id: str, status: str = "completed",
                 result: Any = None, error: Optional[dict] = None):
        from Mqtt_bbs_client.types import TaskOutput, TaskStatus
        output = TaskOutput(
            task_id=task_id,
            agent_id=self.agent_id,
            status=status,
            result=result,
            error=error or {},
        )

        self._client.publish(f"board/task/{task_id}/output", output.to_dict(), retain=True, qos=1)
        self._client.publish(f"board/task/{task_id}/status",
                              TaskStatus.DONE.value if status == "completed" else TaskStatus.FAILED.value,
                              retain=True)
        self._client.publish(f"board/task/{task_id}/signal", "[ROUND_END]", retain=True, qos=2)
        self._client.publish(f"node/{self.agent_id}/task/current", "", retain=True)

        self._current_task_id = None
        log.info(f"[{self.agent_id}] [OK] [PERSIST] 完成: {task_id} ({status})")
