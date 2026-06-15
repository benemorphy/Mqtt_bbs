"""
BBS 业务层 — AgentBoard（主智能体）+ WorkerAgent（工作智能体）

对标 subagent 文件协议的业务语义：
    AgentBoard.post_task()    → 创建任务（写 input.txt）
    AgentBoard.wait_task()    → 等待结果（轮询 output.txt → 改为了订阅推送）
    WorkerAgent.start()       → 启动消息循环（类似 agentmain.py --task）
    WorkerAgent.claim_task()  → 认领任务（创建 task 目录）
    WorkerAgent.stream_out()  → 实时输出（写 stdout/stderr）
    WorkerAgent.complete()    → 完成任务（写 output.txt + [ROUND END]）
"""

import json, time, uuid, logging, threading, hmac, hashlib
from typing import Optional, Callable, Any
from enum import Enum

from Mqtt_bbs_client.client import BBSClient
from Mqtt_bbs_client.types import TaskMessage, TaskOutput, TaskStatus
from Mqtt_bbs_client import config


# ── BBS 公告板通知（BoardClient BBS 协议，默认必选） ──

def _bbs_notify(event: str, task_id: str, detail: dict):
    """通过 BoardClient BBS 协议发布任务事件通知（失败不阻塞，仅告警）"""
    try:
        from Mqtt_bbs_client.board_client import BoardClient
        with BoardClient(f"bbs_notifier_{task_id[:4]}", board="agent-bbs-test") as bbs:
            reg = bbs.register("bbs_notifier", timeout=0.5)
            token = reg.get("token", "")
            if token:
                content = f"[多Agent·{event}] #{task_id} {json.dumps(detail, ensure_ascii=False)[:200]}"
                bbs.post(content, token)
    except Exception as e:
        log.warning(f"[BBS] BoardClient 通知失败 ({event} #{task_id}): {e}")


def _save_brainstorm(task_id: str, topic: str, agent_id: str,
                     perspective: str, idea: str, detail: str = ""):
    """持久化脑暴结果到 brainstorm_sessions 表（失败不阻塞，仅告警）"""
    try:
        import pymysql
        from Mqtt_bbs_client.config import DB_CONFIG
        conn = pymysql.connect(**DB_CONFIG, connect_timeout=3)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO brainstorm_sessions
                   (session_id, topic, agent_id, perspective, idea, detail, status)
                   VALUES (%s, %s, %s, %s, %s, %s, 'completed')""",
                (task_id, topic, agent_id, perspective, idea, detail)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"[BBS] 脑暴持久化失败 (#{task_id} {agent_id}): {e}")


log = logging.getLogger("Mqtt_bbs.bbs")

# P1.4: v2/task 命名空间
V2_TASK_TOPIC = "v2/task"  # v2/task/{task_id}/{subtype}


def _publish_v2_task(topic_v2: str, payload, client, retain=False, qos=0):
    """发布到 v2/task 命名空间（日志审计用，方便后续迁移）"""
    client.publish(topic_v2, payload, retain=retain, qos=qos)


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ── HMAC 任务签名（Zero Trust：防消息篡改） ──

def _calc_hmac(task_id: str, msg_type: str, task_input: dict) -> str:
    """计算任务消息的 HMAC-SHA256 签名"""
    canonical = json.dumps({"task_id": task_id, "type": msg_type, "input": task_input},
                           sort_keys=True, ensure_ascii=False)
    return hmac.new(
        config.HMAC_SECRET.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def _verify_task(payload: dict) -> bool:
    """验证任务消息的 HMAC 签名"""
    sig = payload.pop("_sig", None)
    if not sig:
        return False
    expected = _calc_hmac(
        payload.get("task_id", ""),
        payload.get("type", ""),
        payload.get("input", {}),
    )
    # 常量时间比较防时序攻击
    return hmac.compare_digest(sig, expected)


# ──────────────────────────────────────────────
# AgentBoard — 主智能体（任务发布者）
# ──────────────────────────────────────────────

class AgentBoard:
    """
    主智能体接口。

    对标: 创建 temp/{task_name}/input.txt → 等 output.txt → 读结果

    用法:
        board = AgentBoard("master")
        tid = board.post_task("scan", {"target": "10.0.0.0/24"})
        result = board.wait_task(tid, timeout=120)
    """

    def __init__(self, agent_id: str = "master", host: str = None, port: int = None):
        self.agent_id = agent_id
        from .persistence import BBSClientWithPersistence as _PyBBS
        kw = {}
        if host is not None: kw['host'] = host
        if port is not None: kw['port'] = port
        self._client = _PyBBS(agent_id, **kw)

        # 任务结果缓存：task_id → TaskOutput
        self._results: dict[str, TaskOutput] = {}
        # 任务回调：task_id → callback
        self._callbacks: dict[str, Callable] = {}

    def __enter__(self):
        self._client.connect()
        self._client.wait_connected(5)
        return self

    def __exit__(self, *args):
        self._client.disconnect()

    # ── 发布任务 ──

    def post_task(self, task_type: str, task_input: dict,
                  task_id: Optional[str] = None,
                  priority: int = 3,
                  timeout: int = config.DEFAULT_TASK_TIMEOUT) -> str:
        """
        发布任务到公告板。

        对标: 写入 temp/{task_id}/input.txt

        Returns: task_id
        """
        if task_id is None:
            task_id = f"task_{uuid.uuid4().hex[:8]}"
        if not self._client.is_connected:
            self._client.connect()
            self._client.wait_connected(3)

        msg = TaskMessage(
            task_id=task_id,
            type=task_type,
            input=task_input,
            priority=priority,
            timeout=timeout,
        )

        # 发布 input + 初始状态
        payload = msg.to_dict()
        payload["_sig"] = _calc_hmac(task_id, task_type, task_input)
        self._client.publish(f"board/task/{task_id}/input", payload, retain=True)
        self._client.publish(f"board/task/{task_id}/status", TaskStatus.PENDING.value, retain=True)
        # P1.4 + P0.3: v2/task 双写 + 统一信封
        envelope = BBSClient.build_payload(
            source=self.agent_id, corr_id=task_id,
            action="task_input",
            task_type=task_type, priority=priority, timeout=timeout,
        )
        envelope["payload"] = payload  # 原始 payload 嵌入信封的 payload 字段
        _publish_v2_task(f"{V2_TASK_TOPIC}/{task_id}/input", envelope, self._client, retain=True, qos=1)
        _publish_v2_task(f"{V2_TASK_TOPIC}/{task_id}/status",
                         BBSClient.build_payload(source=self.agent_id, corr_id=task_id,
                                                  action="task_status", status=TaskStatus.PENDING.value),
                         self._client, retain=True)

        # 也发布到 open 索引（待认领列表）— P0速赢: retain=True 确保新 Worker 重启后能拉取
        self._client.publish(f"board/open", task_id, retain=True)
        _publish_v2_task(f"{V2_TASK_TOPIC}/open",
                         BBSClient.build_payload(source=self.agent_id, corr_id=task_id,
                                                  action="task_open", task_id=task_id),
                         self._client, retain=True)

        log.info(f"[{self.agent_id}] [OUT] 发布任务: {task_id} ({task_type})")

        # BBS 公告板通知（默认必选）
        _bbs_notify("TASK_CREATED", task_id, {
            "agent": self.agent_id, "type": task_type,
            "input_preview": str(task_input)[:100],
        })

        return task_id

    # ── 能力查询 ──

    def query_capabilities(self, capability: Optional[str] = None,
                           timeout: float = 5.0) -> list[dict]:
        """
        查询在线 Agent 及其能力。

        向 CapabilityRegistry (BoardService) 发送查询请求，
        等待返回注册表快照。

        Args:
            capability: 按能力过滤，None=返回全部
            timeout: 等待超时(秒)

        Returns: list[dict] — 每个 dict 含 agent_id, capabilities, status, last_seen
        """
        corr_id = f"q_{uuid.uuid4().hex[:8]}"
        result_holder = {"agents": None}

        def on_response(topic, payload):
            if isinstance(payload, dict) and payload.get("agents"):
                result_holder["agents"] = payload["agents"]

        resp_topic = f"board/capability/query/response/{corr_id}"
        self._client.subscribe(resp_topic, on_response)

        # P0.3: 使用统一信封发送查询
        self._client.publish("board/capability/query", BBSClient.build_payload(
            source=self.agent_id, corr_id=corr_id, reply_to=f"board/capability/query/response/",
            action="capability_query", capability=capability,
        ))

        # 等待响应
        deadline = time.time() + timeout
        while time.time() < deadline and result_holder["agents"] is None:
            time.sleep(0.1)

        self._client.unsubscribe(resp_topic)
        agents = result_holder["agents"] or []
        log.info(f"[{self.agent_id}] [QUERY] 能力查询: filter={capability} → {len(agents)} agents")
        return agents

    # ── 路由发布任务 ──

    def post_task_routed(self, task_type: str, task_input: dict,
                         target_agent_id: Optional[str] = None,
                         target_capability: Optional[str] = None,
                         task_id: Optional[str] = None,
                         priority: int = 3,
                         timeout: int = config.DEFAULT_TASK_TIMEOUT) -> str:
        """
        发布任务到公告板，支持能力路由和定向分发。

        相比 post_task()，额外支持:
        - target_agent_id: 直接推送到指定 Agent
        - target_capability: 推送到有该能力的所有在线 Agent

        向后兼容：同时保留 board/task/{id}/input 广播，旧 Worker 仍能接收。
        """
        tid = self.post_task(task_type, task_input, task_id, priority, timeout)

        if target_agent_id:
            # 定向推送到指定 Agent 的 node topic (带统一信封)
            self._client.publish(
                f"node/{target_agent_id}/task/input",
                BBSClient.build_payload(
                    source=self.agent_id, corr_id=tid,
                    action="task_routed",
                    task_id=tid, type=task_type, input=task_input,
                ),
                retain=False
            )
            log.info(f"[{self.agent_id}] 定向分发: {tid} -> {target_agent_id}")

        elif target_capability:
            # 查询有该能力的 Agent，逐个推送
            agents = self.query_capabilities(target_capability, timeout=3)
            online = [a for a in agents if a.get("status") == "online"]
            for agent in online:
                aid = agent["agent_id"]
                self._client.publish(
                    f"node/{aid}/task/input",
                    {"task_id": tid, "type": task_type, "input": task_input},
                    retain=False
                )
            log.info(f"[{self.agent_id}] [IN] 能力路由: {tid} → {len(online)} agents ({target_capability})")

        return tid

    # ── 等待结果 ──

    def wait_task(self, task_id: str, timeout: Optional[float] = None,
                  poll_interval: float = 0.5) -> TaskOutput:
        """
        等待任务完成。

        对标: 轮询 temp/{task_id}/output.txt + 检测 [ROUND END]

        通过订阅 task/{id}/signal 和 task/{id}/output 实现实时推送。

        Returns: TaskOutput
        """
        if timeout is None:
            timeout = config.DEFAULT_TASK_TIMEOUT

        result_holder = {"output": None}

        def on_output(topic, payload):
            if isinstance(payload, dict):
                result_holder["output"] = TaskOutput.from_dict(payload)

        def on_signal(topic, payload):
            signal = payload
            if isinstance(payload, bytes):
                signal = payload.decode("utf-8")
            if signal == "[ROUND_END]":
                # signal 收到后，output 应该在 Retain 中
                pass

        # 订阅 output 和 signal（board + v2 双订阅）
        self._client.subscribe(f"board/task/{task_id}/output", on_output)
        self._client.subscribe(f"board/task/{task_id}/signal", on_signal)
        self._client.subscribe(f"{V2_TASK_TOPIC}/{task_id}/output", on_output)
        self._client.subscribe(f"{V2_TASK_TOPIC}/{task_id}/signal", on_signal)

        # 先尝试读 Retain（任务可能已经完成）
        # paho 的 subscribe 会自动收到 Retain 消息

        deadline = time.time() + timeout
        while time.time() < deadline:
            if result_holder["output"] is not None:
                self._client.unsubscribe(f"board/task/{task_id}/output")
                self._client.unsubscribe(f"board/task/{task_id}/signal")
                self._client.unsubscribe(f"{V2_TASK_TOPIC}/{task_id}/output")
                self._client.unsubscribe(f"{V2_TASK_TOPIC}/{task_id}/signal")
                log.info(f"[{self.agent_id}] [OK] 任务完成: {task_id}")
                return result_holder["output"]
            time.sleep(poll_interval)

        # 超时
        self._client.unsubscribe(f"board/task/{task_id}/output")
        self._client.unsubscribe(f"board/task/{task_id}/signal")
        self._client.unsubscribe(f"{V2_TASK_TOPIC}/{task_id}/output")
        self._client.unsubscribe(f"{V2_TASK_TOPIC}/{task_id}/signal")
        log.warning(f"[{self.agent_id}] ⏰ 任务超时: {task_id}")
        return TaskOutput(task_id=task_id, agent_id="", status="failed",
                          error={"type": "timeout", "msg": f"等待超过{timeout}秒"})

    # ── 取消任务 ──

    def cancel_task(self, task_id: str):
        """发送取消信号"""
        self._client.publish(f"board/task/{task_id}/signal", "[CANCEL]", retain=True, qos=2)
        self._client.publish(f"board/task/{task_id}/status", TaskStatus.CANCELLED.value, retain=True)
        log.info(f"[{self.agent_id}] [STOP] 取消任务: {task_id}")

    # ── 公开发布（Point 5: Broadcast） ──

    def publish(self, topic: str, payload: Any, retain: bool = False, qos: Optional[int] = None):
        """发布消息到任意 topic（对标 README 的 board.publish()）"""
        self._client.publish(topic, payload, retain=retain, qos=qos)

    # ── 公开订阅（Point 4: Wildcard 订阅收集） ──

    def subscribe(self, topic: str, callback: Callable):
        """订阅任意 topic（对标通配符订阅收集结果）"""
        self._client.subscribe(topic, callback)

    # ── Map-Reduce 批量等待 + 自动聚合（Point 4） ──

    def wait_all(self, task_ids: list[str], timeout: float = 300,
                 reduce_fn: Optional[Callable] = None,
                 poll_interval: float = 0.5) -> Any:
        """
        批量等待多个任务完成，自动聚合结果。

        对标: 通配符订阅 board/task/+/output 收集结果

        Args:
            task_ids: 任务ID列表
            timeout: 总超时（秒）
            reduce_fn: 聚合函数(list[TaskOutput]) → any，默认返回 list
            poll_interval: 轮询间隔

        Returns:
            reduce_fn 的结果，或 TaskOutput 列表
        """
        deadline = time.time() + timeout
        results: dict[str, TaskOutput] = {}

        def on_output(topic, payload):
            if isinstance(payload, dict):
                out = TaskOutput.from_dict(payload)
                results[out.task_id] = out

        # 为每个 task_id 订阅 output
        remaining = set(task_ids)
        for tid in task_ids:
            self._client.subscribe(f"board/task/{tid}/output", on_output)

        # 等待所有完成
        while remaining and time.time() < deadline:
            for tid in list(remaining):
                if tid in results and results[tid].status in ("completed", "failed", "cancelled"):
                    remaining.discard(tid)
            if not remaining:
                break
            time.sleep(poll_interval)

        # 清理订阅
        for tid in task_ids:
            self._client.unsubscribe(f"board/task/{tid}/output")

        # 超时未完成的任务标记为失败
        for tid in remaining:
            results[tid] = TaskOutput(
                task_id=tid, agent_id="", status="failed",
                error={"type": "timeout", "msg": f"wait_all 超时 ({timeout}s)"},
            )

        ordered = [results.get(tid, TaskOutput(
            task_id=tid, agent_id="", status="failed",
            error={"type": "lost", "msg": "任务结果丢失"},
        )) for tid in task_ids]

        if reduce_fn:
            return reduce_fn(ordered)
        return ordered


# ──────────────────────────────────────────────
# WorkerAgent — 工作智能体（任务执行者）
# ──────────────────────────────────────────────

class WorkerAgent:
    """
    工作智能体接口。

    对标: agentmain.py --task {name} → 读 input.txt → 执行 → 写 output.txt → [ROUND END]

    用法:
        worker = WorkerAgent("agent_alpha", capabilities=["scan", "analyse"])
        worker.on_task(lambda task: {"result": "ok"})
        worker.start()  # 进入消息循环
    """

    def __init__(self, agent_id: str, capabilities: Optional[list[str]] = None,
                 host: str = config.BROKER_HOST, port: int = config.BROKER_PORT):
        self.agent_id = agent_id
        self.capabilities = capabilities or []
        # 惰性导入避免循环
        from .persistence import BBSClientWithPersistence as _PyBBS
        self._client = _PyBBS(agent_id, host=host, port=port)
        self._task_handler: Optional[Callable] = None
        self._running = False
        self._current_task_id: Optional[str] = None
        self._seq = 0  # stdout/stderr 序列号
        self._interventions: list[dict] = []  # 运行时注入命令队列（Point 6）
        self._suspended = False  # 暂停标志（Point 5: Broadcast）
        self._subscribed_dynamic: set[str] = set()  # 动态订阅 topic 集合
        self._current_task_msg: Optional["TaskMessage"] = None  # 当前任务消息（用于持久化）

    # ── 注册任务处理器 ──

    def on_task(self, handler: Callable[[TaskMessage], Any]):
        """
        注册任务处理函数。

        handler 接收 TaskMessage，返回结果（将被写入 output）。
        handler 也可以调用 self.stream_out() / self.stream_err() 实时输出。
        """
        self._task_handler = handler
        return self

    # ── 启动 ──

    def start(self, block: bool = True):
        """
        启动工作智能体。

        - 发布能力声明到 node/{id}/capability
        - 订阅 board/task/+/input 等待任务
        - block=True 时进入阻塞循环

        对标: subagent 启动后等待任务分配
        """
        self._client.connect()
        self._client.wait_connected(5)
        self._running = True

        # 发布能力声明
        self._client.publish(f"node/{self.agent_id}/capability",
                             {"agent_id": self.agent_id, "capabilities": self.capabilities},
                             retain=True)

        # 发布在线状态
        self._client.publish(f"node/{self.agent_id}/status", "online", retain=True)

        # 订阅所有任务的 input（含待认领 + 新发布）— board + v2 双订阅
        self._client.subscribe("board/task/+/input", self._on_task_input)
        self._client.subscribe(f"{V2_TASK_TOPIC}/+/input", self._on_task_input)

        # 订阅定向任务（能力市场路由专用）
        self._client.subscribe(f"node/{self.agent_id}/task/input", self._on_directed_task)

        # 订阅取消信号
        self._client.subscribe(f"node/{self.agent_id}/task/current", self._on_cancel)
        # 订阅全局广播信号（Point 5: Broadcast/Multicast）
        self._client.subscribe("board/global/signal", self._on_global_signal)

        log.info(f"[{self.agent_id}] [START] 启动 (capabilities={self.capabilities})")

        if block:
            try:
                while self._running:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.stop()

    def stop(self):
        """停止工作智能体"""
        self._running = False
        self._client.publish(f"node/{self.agent_id}/status", "offline", retain=True)
        self._client.disconnect()
        log.info(f"[{self.agent_id}] [STOP] 停止")

    # ── 认领任务 ──

    def claim_task(self, task_id: str) -> bool:
        """
        认领任务。

        对标: agentmain.py --task {task_id} → 创建 temp/{task_id}/目录
        """
        self._current_task_id = task_id
        self._seq = 0

        # 发布 claim + 状态（board + v2 双写）
        self._client.publish(f"board/task/{task_id}/claim",
                             {"agent_id": self.agent_id, "claimed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                             retain=True)
        self._client.publish(f"board/task/{task_id}/status", TaskStatus.RUNNING.value, retain=True)
        _publish_v2_task(f"{V2_TASK_TOPIC}/{task_id}/claim",
                         {"agent_id": self.agent_id, "claimed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                         self._client, retain=True, qos=1)
        _publish_v2_task(f"{V2_TASK_TOPIC}/{task_id}/status", TaskStatus.RUNNING.value, self._client, retain=True)
        self._client.publish(f"node/{self.agent_id}/task/current", task_id, retain=True)
        self._client.publish(f"node/{self.agent_id}/status", "busy", retain=True)

        # 动态订阅：任务取消信号 + 运行时注入（board + v2）
        self._client.subscribe(f"board/task/{task_id}/signal", self._on_task_signal)
        self._subscribed_dynamic.add(f"board/task/{task_id}/signal")
        self._client.subscribe(f"board/task/{task_id}/intervene", self._on_intervene)
        self._subscribed_dynamic.add(f"board/task/{task_id}/intervene")
        self._client.subscribe(f"{V2_TASK_TOPIC}/{task_id}/signal", self._on_task_signal)
        self._subscribed_dynamic.add(f"{V2_TASK_TOPIC}/{task_id}/signal")
        self._client.subscribe(f"{V2_TASK_TOPIC}/{task_id}/intervene", self._on_intervene)
        self._subscribed_dynamic.add(f"{V2_TASK_TOPIC}/{task_id}/intervene")

        log.info(f"[{self.agent_id}] [HAND] 认领任务: {task_id}")

        # BBS 公告板通知（默认必选）
        _bbs_notify("TASK_CLAIMED", task_id, {
            "agent": self.agent_id,
            "capabilities": self.capabilities,
        })

        return True

    # ── 流式输出 ──

    def stream_out(self, task_id_or_data=None, data: str = None):
        """
        实时标准输出。

        兼容双签名:
            stream_out("text")                # 用 self._current_task_id
            stream_out(task_id, "text")       # 显式指定task_id

        对标: print / logger 写入 stdout
        """
        self._seq += 1
        if data is None and isinstance(task_id_or_data, str):
            # stream_out("text")
            tid = self._current_task_id
            text = task_id_or_data
        else:
            # stream_out(task_id, "text")
            tid = task_id_or_data
            text = data
        if tid:
            self._client.publish(f"board/task/{tid}/stdout",
                                 {"seq": self._seq, "data": text}, retain=False)
            _publish_v2_task(f"{V2_TASK_TOPIC}/{tid}/stdout",
                             {"seq": self._seq, "data": text}, self._client, retain=False)

    def stream_err(self, task_id_or_data=None, data: str = None):
        self._seq += 1
        if data is None and isinstance(task_id_or_data, str):
            tid = self._current_task_id; text = task_id_or_data
        else:
            tid = task_id_or_data; text = data
        if tid:
            self._client.publish(f"board/task/{tid}/stderr", {"seq": self._seq, "data": text}, retain=False)
            _publish_v2_task(f"{V2_TASK_TOPIC}/{tid}/stderr",
                             {"seq": self._seq, "data": text}, self._client, retain=False)

    # ── 完成任务 ──

    def complete(self, task_id_or_result=None, status: str = "completed",
                 result: Any = None, error: Optional[dict] = None):
        """
        完成任务。

        兼容双签名:
            complete(result={"ok": True})            # 原版无task_id
            complete(task_id, result={"ok": True})   # 新版带task_id

        对标: 写入 output.txt → 追加 [ROUND END]
        """
        if result is None and isinstance(task_id_or_result, dict):
            # 兼容: complete({"ok": True})
            result = task_id_or_result
            task_id = self._current_task_id
        elif result is not None and isinstance(task_id_or_result, str):
            # 兼容: complete(task_id, result={"ok": True})
            task_id = task_id_or_result
        elif task_id_or_result is None:
            task_id = self._current_task_id
        else:
            task_id = self._current_task_id
        if not task_id:
            return

        output = TaskOutput(
            task_id=task_id,
            agent_id=self.agent_id,
            status=status,
            result=result,
            error=error,
            metrics={"duration_sec": 0},  # TODO: 可加计时
        )

        # 写 output（对标 output.txt）— board + v2 双写
        self._client.publish(f"board/task/{task_id}/output", output.to_dict(), retain=True, qos=1)
        _publish_v2_task(f"{V2_TASK_TOPIC}/{task_id}/output", output.to_dict(), self._client, retain=True, qos=1)

        # 发完成信号（对标 [ROUND END]）
        self._client.publish(f"board/task/{task_id}/signal", "[ROUND_END]", retain=True, qos=2)
        _publish_v2_task(f"{V2_TASK_TOPIC}/{task_id}/signal", "[ROUND_END]", self._client, retain=True, qos=2)

        # 更新状态
        task_status = TaskStatus.DONE if status == "completed" else TaskStatus.FAILED
        self._client.publish(f"board/task/{task_id}/status", task_status.value, retain=True)
        _publish_v2_task(f"{V2_TASK_TOPIC}/{task_id}/status", task_status.value, self._client, retain=True)

        # ── P0速赢: 清理 task retain 堆积 ──
        # 任务完成后清除 retain，避免 broker 堆积过期 retained 消息
        for _clean_topic in [f"board/task/{task_id}/output",
                              f"board/task/{task_id}/signal",
                              f"board/task/{task_id}/status",
                              f"{V2_TASK_TOPIC}/{task_id}/output",
                              f"{V2_TASK_TOPIC}/{task_id}/signal",
                              f"{V2_TASK_TOPIC}/{task_id}/status"]:
            self._client.publish(_clean_topic, "", retain=True)

        # 取消动态订阅（任务信号 + intervene）
        self._unsubscribe_dynamic()
        # 清理干预队列
        self._interventions.clear()

        # 清理自身状态
        self._client.publish(f"node/{self.agent_id}/task/current", "", retain=True)
        self._client.publish(f"node/{self.agent_id}/status", "online", retain=True)

        # 脑暴结果持久化（自动检测 brainstorm 任务类型）
        if self._current_task_msg and getattr(self._current_task_msg, 'type', '') == 'brainstorm':
            topic = ""
            if hasattr(self._current_task_msg, 'input') and isinstance(self._current_task_msg.input, dict):
                topic = self._current_task_msg.input.get("topic", "")
            persp = result.get("perspective", "") if isinstance(result, dict) else ""
            idea = result.get("idea", "") if isinstance(result, dict) else str(result or "")
            detail = result.get("detail", "") if isinstance(result, dict) else ""
            _save_brainstorm(task_id, topic, self.agent_id, persp, idea, detail)
        self._current_task_msg = None  # 清理

        log.info(f"[{self.agent_id}] [OK] 任务完成: {task_id} (status={status})")

        # BBS 公告板通知（默认必选）
        _bbs_notify("TASK_COMPLETED", task_id, {
            "agent": self.agent_id, "status": status,
            "result_preview": str(result)[:100] if result else "",
        })

        self._current_task_id = None

    # ── 动态订阅管理（Point 4/6） ──

    def _unsubscribe_dynamic(self):
        """取消所有动态订阅"""
        for topic in list(self._subscribed_dynamic):
            self._client.unsubscribe(topic)
        self._subscribed_dynamic.clear()

    # ── 公开方法：获取干预命令（Point 6） ──

    def get_interventions(self) -> list[dict]:
        """
        获取并清空运行时注入的干预命令。

        在 task_handler 中定期调用，检查是否有干预指令:

            for cmd in worker.get_interventions():
                if cmd.get("action") == "skip":
                    ...
        """
        result = list(self._interventions)
        self._interventions.clear()
        return result

    # ── 任务信号处理（Point 6: 修复 cancel 路由） ──

    def _on_task_signal(self, topic: str, payload):
        """收到 board/task/{task_id}/signal 信号"""
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        if payload == "[CANCEL]" and self._current_task_id:
            log.warning(f"[{self.agent_id}] [STOP] 收到取消信号 (board/task信号)")
            self.complete(status="failed", error={"type": "cancelled", "msg": "被主智能体取消"})

    # ── 运行时注入处理（Point 6: Intervene） ──

    def _on_intervene(self, topic: str, payload):
        """收到运行时注入命令 → 存入干预队列"""
        if isinstance(payload, bytes):
            try:
                payload = json.loads(payload.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                payload = {"raw": payload.decode("utf-8", errors="replace")}
        if not isinstance(payload, dict):
            payload = {"action": str(payload)}
        payload["_received_at"] = time.time()
        self._interventions.append(payload)
        log.info(f"[{self.agent_id}] [MSG] 收到干预命令: {payload.get('action', 'unknown')}")

    # ── 全局广播信号处理（Point 5: Broadcast） ──

    def _on_global_signal(self, topic: str, payload):
        """收到全局广播信号: [SUSPEND] / [RESUME] / [SHUTDOWN]"""
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        if payload == "[SUSPEND]":
            self._suspended = True
            log.warning(f"[{self.agent_id}] ⏸ 全局暂停")
        elif payload == "[RESUME]":
            self._suspended = False
            log.info(f"[{self.agent_id}] ▶ 全局恢复")
        elif payload == "[SHUTDOWN]":
            log.warning(f"[{self.agent_id}] [STOP] 全局关机")
            self.stop()

    # ── 内部消息处理 ──

    def _on_directed_task(self, topic: str, payload):
        """收到定向任务（能力市场路由）"""
        if not self._task_handler:
            return
        if not isinstance(payload, dict):
            return
        task_id = payload.get("task_id", "")
        task_type = payload.get("type", "")
        task_input = payload.get("input", {})
        if not task_id or not task_type:
            return

        # 能力匹配检查
        if self.capabilities and task_type not in self.capabilities:
            log.debug(f"[{self.agent_id}] ⏭ 跳过定向不匹配: {task_type}")
            return

        # 检查是否已被认领
        from Mqtt_bbs_client.types import TaskMessage
        msg = TaskMessage(task_id=task_id, type=task_type, input=task_input)
        self._current_task_msg = msg
        self.claim_task(task_id)
        try:
            log.info(f"[{self.agent_id}] ▶ 执行定向任务: {task_id} ({task_type})")
            result = self._task_handler(msg)
            self.complete(result=result)
        except Exception as e:
            log.error(f"[{self.agent_id}] [FAIL] 定向任务异常: {e}")
            self.complete(status="failed", error={"type": "exception", "msg": str(e)})

    def _on_task_input(self, topic: str, payload):
        """收到任务 input → 零信任验签 → 能力匹配 → 自动认领"""
        if not self._task_handler:
            return

        if not isinstance(payload, dict):
            return

        # 零信任验签：HMAC 签名验证
        payload_copy = dict(payload)  # 不修改原数据
        if not _verify_task(payload_copy):
            log.warning(f"[{self.agent_id}] [LOCK] 签名无效，拒绝任务: topic={topic}")
            return

        # 提取 task_id 从 topic "board/task/{task_id}/input"
        parts = topic.split("/")
        if len(parts) >= 3:
            task_id = parts[2]
        else:
            return

        msg = TaskMessage.from_dict(payload)
        self._current_task_msg = msg  # 记住任务消息（用于持久化）

        # 能力匹配检查
        if self.capabilities and msg.type not in self.capabilities:
            log.debug(f"[{self.agent_id}] ⏭ 跳过不匹配任务: {msg.type} (我有: {self.capabilities})")
            return

        # 自动认领
        self.claim_task(task_id)

        # 执行
        try:
            log.info(f"[{self.agent_id}] ▶ 执行任务: {task_id} ({msg.type})")
            self.stream_out(f"开始执行: {msg.type}")
            result = self._task_handler(msg)
            self.complete(result=result)
        except Exception as e:
            log.error(f"[{self.agent_id}] [FAIL] 任务异常: {e}")
            self.stream_err(f"异常: {str(e)}")
            self.complete(status="failed", error={"type": "exception", "msg": str(e)})

    def _on_cancel(self, topic: str, payload):
        """收到取消信号"""
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        if payload == "[CANCEL]" and self._current_task_id:
            log.warning(f"[{self.agent_id}] [STOP] 收到取消信号")
            self.complete(status="failed", error={"type": "cancelled", "msg": "被主智能体取消"})
