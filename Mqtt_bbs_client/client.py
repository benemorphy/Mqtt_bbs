"""
MQTT BBS Client 层 — 对标 subagent 文件协议

文件协议 → MQTT 映射:
    input.txt          →  PUBLISH topic/input    [Retain=True]
    output.txt         →  PUBLISH topic/output   [Retain=True]
    [ROUND END]        →  PUBLISH topic/signal   [Retain=True, QoS=2]
    temp/{name}/       →  topic/ 主题空间隔离
    轮询读文件          →  Subscribe 对应主题，回调通知
    PID标识进程         →  node/{agent_id}/task/current
    进程异常退出        →  LWT 自动发布 offline
    stdout/stderr      →  topic/stdout, topic/stderr (流式, QoS=0)
"""

import json, time, uuid, logging, os, threading
from typing import Callable, Optional, Any
from dataclasses import dataclass, field

import paho.mqtt.client as mqtt

from . import config
from .types import TaskMessage, TaskOutput

log = logging.getLogger("Mqtt_bbs")

# 延迟导入避免循环依赖
_rate_limiter_instance = None
_audit_logger_instance = None


def _get_rate_limiter():
    """获取或创建全局速率限制器"""
    global _rate_limiter_instance
    if _rate_limiter_instance is None:
        from .rate_limiter import RateLimiter
        _rate_limiter_instance = RateLimiter(
            max_per_sec=config.RATE_LIMIT_MAX_PER_SEC,
            burst=config.RATE_LIMIT_BURST,
            enabled=config.RATE_LIMIT_ENABLED,
        )
    return _rate_limiter_instance


def _get_audit_logger(client=None):
    """获取或创建全局审计日志记录器"""
    global _audit_logger_instance
    if _audit_logger_instance is None:
        from .audit_log import AuditLogger
        _audit_logger_instance = AuditLogger(
            mqtt_client=client,
            log_topic=config.AUDIT_LOG_TOPIC,
            enabled=config.AUDIT_LOG_ENABLED,
        )
    return _audit_logger_instance


# ──────────────────────────────────────────────
# MQTT Client 封装层
# ──────────────────────────────────────────────

class BBSClient:
    """
    MQTT 客户端封装。

    对标 subagent 中 '创建 input.txt → 等 output.txt → 检测 [ROUND END]' 的模式，
    替换为: PUBLISH → Subscribe → callback 通知。

    Args:
        agent_id: 智能体ID（用于 node/{agent_id}/... 主题和 LWT）
        host: MQTT Broker 地址
        port: MQTT Broker 端口
        mqtt_version: MQTT 协议版本 (5=MQTTv5, 4=MQTTv3.1.1)
        clean_start: MQTTv5 clean_start（True=丢弃旧会话）
        session_expiry_interval: 会话过期时间（秒，0=立即过期）
        username: MQTT 用户名（默认从环境变量 MQTT_USERNAME 读取）
        password: MQTT 密码/JWT Token（默认从环境变量 MQTT_PASSWORD 读取）
    """

    def __init__(
        self,
        agent_id: str,
        host: str = config.BROKER_HOST,
        port: int = config.BROKER_PORT,
        mqtt_version: int = 5,
        clean_start: bool = True,
        session_expiry_interval: int = 0,
        username: Optional[str] = None,
        password: Optional[str] = None,
        tls_enabled: Optional[bool] = None,
        tls_ca_certs: Optional[str] = None,
        tls_certfile: Optional[str] = None,
        tls_keyfile: Optional[str] = None,
        tls_insecure: Optional[bool] = None,
    ):
        self.agent_id = agent_id
        self.host = host or config.BROKER_HOST
        self.port = port or config.BROKER_PORT
        self.mqtt_version = mqtt_version
        self.clean_start = clean_start
        self.session_expiry_interval = session_expiry_interval
        self._prefix = config.TOPIC_PREFIX

        # 认证凭据（优先参数 > 环境变量）
        self._username = username or os.environ.get("MQTT_USERNAME")
        self._password = password or os.environ.get("MQTT_PASSWORD")

        # TLS 配置（优先参数 > 环境变量）
        self._tls_enabled = tls_enabled if tls_enabled is not None else config.MQTT_TLS_ENABLED
        self._tls_ca_certs = tls_ca_certs or config.MQTT_TLS_CA_CERTS
        self._tls_certfile = tls_certfile or config.MQTT_TLS_CERTFILE
        self._tls_keyfile = tls_keyfile or config.MQTT_TLS_KEYFILE
        self._tls_insecure = tls_insecure if tls_insecure is not None else config.MQTT_TLS_INSECURE

        # paho 客户端
        try:
            proto = mqtt.MQTTv5 if mqtt_version == 5 else mqtt.MQTTv311
            self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, agent_id, protocol=proto)
        except TypeError:
            self._client = mqtt.Client(client_id=agent_id)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        # 设置 LWT（智能体异常断开时自动发布离线状态）
        will_props = None
        if self.mqtt_version == 5:
            will_props = mqtt.Properties(mqtt.PacketTypes.WILLMESSAGE)
            will_props.MessageExpiryInterval = 300
        self._client.will_set(
            f"{self._prefix}node/{agent_id}/status",
            payload="offline",
            qos=1,
            retain=True,
            properties=will_props if self.mqtt_version == 5 else None,
        )

        # 回调注册表：topic_pattern → [callbacks]
        self._subscriptions: dict[str, list[Callable]] = {}
        self._connected = False
        self._loop_started = False

        # 速率限制器 & 审计日志
        self._rate_limiter = _get_rate_limiter()
        self._audit_logger = _get_audit_logger(self)

    # ── 安全属性 ──

    @property
    def rate_limiter(self):
        """速率限制器实例"""
        return self._rate_limiter

    @property
    def audit_logger(self):
        """审计日志记录器实例"""
        return self._audit_logger

        # 速率限制器 & 审计日志
        self._rate_limiter = _get_rate_limiter()
        self._audit_logger = _get_audit_logger(self)

    # ── 连接管理 ──

    def connect(self):
        """连接到 MQTT Broker（非阻塞）"""
        # TLS 加密传输
        if self._tls_enabled:
            import ssl as _ssl
            tls_kwargs = {}
            if self._tls_ca_certs:
                tls_kwargs['ca_certs'] = self._tls_ca_certs
            if self._tls_certfile:
                tls_kwargs['certfile'] = self._tls_certfile
            if self._tls_keyfile:
                tls_kwargs['keyfile'] = self._tls_keyfile

            # 证书验证级别
            cert_reqs_map = {
                "CERT_REQUIRED": _ssl.CERT_REQUIRED,
                "CERT_OPTIONAL": _ssl.CERT_OPTIONAL,
                "CERT_NONE": _ssl.CERT_NONE,
            }
            tls_kwargs['cert_reqs'] = cert_reqs_map.get(
                config.MQTT_TLS_CERT_REQS, _ssl.CERT_REQUIRED
            )

            # TLS 版本
            tls_version_map = {
                "tlsv1.2": _ssl.PROTOCOL_TLSv1_2,
                "tlsv1.3": _ssl.PROTOCOL_TLS,
                "tls": _ssl.PROTOCOL_TLS,
            }
            tls_kwargs['tls_version'] = tls_version_map.get(
                config.MQTT_TLS_VERSION, _ssl.PROTOCOL_TLS
            )

            self._client.tls_set(**tls_kwargs)

            # 不安全模式（跳过证书主机名校验）
            if self._tls_insecure:
                self._client.tls_insecure_set(True)
                log.warning(f"[{self.agent_id}] TLS insecure mode enabled — skipping certificate hostname verification")

        # Zero Trust: 设置认证凭据
        if self._username:
            self._client.username_pw_set(self._username, self._password)

        if self.mqtt_version == 5:
            # MQTT 5.0: 使用 Properties 传递会话配置
            props = mqtt.Properties(mqtt.PacketTypes.CONNECT)
            props.SessionExpiryInterval = self.session_expiry_interval
            self._client.connect(self.host, self.port, config.KEEPALIVE,
                                 clean_start=self.clean_start, properties=props)
        else:
            # MQTT 3.1.1: 使用 clean_session
            self._client.connect(self.host, self.port, config.KEEPALIVE)

        if not self._loop_started:
            self._client.loop_start()
            self._loop_started = True
        log.info(f"[{self.agent_id}] 连接中 {self.host}:{self.port} (MQTTv{'5' if self.mqtt_version==5 else '3.1.1'})")

    def disconnect(self):
        """断开连接"""
        self._client.disconnect()
        if self._loop_started:
            self._client.loop_stop()
            self._loop_started = False
        log.info(f"[{self.agent_id}] 已断开")

    def wait_connected(self, timeout: float = 5.0) -> bool:
        """阻塞等待连接建立"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._connected:
                return True
            time.sleep(0.1)
        return self._connected

    # ── 心跳 ──

    def start_heartbeat(self, interval: int = None, capabilities: list = None):
        """启动心跳线程，定期发布 node/{agent_id}/heartbeat

        Args:
            interval: 心跳间隔(秒)，默认 config.HEARTBEAT_INTERVAL
            capabilities: Agent 能力列表（可选）
        """
        if getattr(self, '_hb_thread', None) and self._hb_thread.is_alive():
            return
        self._hb_interval = interval or config.HEARTBEAT_INTERVAL
        self._hb_caps = capabilities or []
        self._hb_running = True
        self._hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._hb_thread.start()
        log.info(f"[{self.agent_id}] [HB] 心跳已启动 (每{self._hb_interval}s)")

    def stop_heartbeat(self):
        """停止心跳线程"""
        self._hb_running = False
        log.info(f"[{self.agent_id}] [HB] 心跳已停止")

    def _heartbeat_loop(self):
        """心跳循环"""
        while self._hb_running and self._connected:
            try:
                payload = {
                    "agent_id": self.agent_id,
                    "timestamp": time.time(),
                    "status": "online",
                    "capabilities": self._hb_caps,
                }
                self.publish(f"node/{self.agent_id}/heartbeat", payload,
                             retain=False, qos=1)
            except Exception as e:
                log.warning(f"[{self.agent_id}] [HB] 心跳发送异常: {e}")
            time.sleep(self._hb_interval)

    # ── 发布 ──

    @staticmethod
    def build_payload(source: str, corr_id: str, reply_to: str = "", action: str = "",
                      **extra) -> dict:
        """构造标准化消息信封: {v, action, source, corr_id, reply_to, ...extra}

        统一所有模块的消息格式，通过 corr_id+reply_to 定位响应。
        业务字段通过 **extra 传入，保持向后兼容。

        用法:
            BBSClient.build_payload(
                source="agent_alpha", corr_id="abc", reply_to="v2/agent/alpha/rpc/res/",
                action="register", agent_id="agent_alpha", name="my_agent",
            )
            # => {"v": 1, "action": "register", "source": "agent_alpha",
            #     "corr_id": "abc", "reply_to": "v2/agent/alpha/rpc/res/",
            #     "agent_id": "agent_alpha", "name": "my_agent"}
        """
        return {
            "v": 1,
            "action": action,
            "source": source,
            "corr_id": corr_id,
            "reply_to": reply_to,
            **extra,
        }

    def publish(self, topic_suffix: str, payload: Any, retain: bool = False, qos: Optional[int] = None,
                properties: Optional[dict] = None):
        """
        发布消息到 agent/{topic_suffix}。

        Args:
            topic_suffix: 主题后缀，如 "board/task/task_001/input"
            payload: 消息内容（str/dict/bytes）
            retain: 是否保留（对标文件持久化）
            qos: 服务质量，默认按场景从 config.QOS 获取
            properties: MQTT 5.0 Properties 字典
                       支持: message_expiry, user_properties (list of (k,v) tuples)
        """
        topic = f"{self._prefix}{topic_suffix}"
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, ensure_ascii=False)
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        if qos is None:
            # 从 topic 后缀猜测 QoS
            for key, val in config.QOS.items():
                if key in topic_suffix:
                    qos = val
                    break
            if qos is None:
                qos = 1

        # MQTT 5.0 Properties
        pub_props = None
        if properties and self.mqtt_version == 5:
            pub_props = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
            if "message_expiry" in properties:
                pub_props.MessageExpiryInterval = properties["message_expiry"]
            if "user_properties" in properties:
                pub_props.UserProperty = properties["user_properties"]

        info = self._client.publish(topic, payload, qos=qos, retain=retain, properties=pub_props)
        if pub_props:
            log.debug(f"[PUB] {topic} (qos={qos}, retain={retain}, props={properties})")
        else:
            log.debug(f"[PUB] {topic} (qos={qos}, retain={retain})")
        return info

    def publish(self, topic_suffix: str, payload: Any, retain: bool = False, qos: Optional[int] = None,
                properties: Optional[dict] = None, bypass_rate_limit: bool = False):
        """
        发布消息到 agent/{topic_suffix}。

        Args:
            topic_suffix: 主题后缀，如 "board/task/task_001/input"
            payload: 消息内容（str/dict/bytes）
            retain: 是否保留（对标文件持久化）
            qos: 服务质量，默认按场景从 config.QOS 获取
            properties: MQTT 5.0 Properties 字典
                       支持: message_expiry, user_properties (list of (k,v) tuples)
            bypass_rate_limit: 是否绕过速率限制（用于心跳等关键消息）
        """
        # 速率限制检查
        if not bypass_rate_limit and not self._rate_limiter.allow(topic=topic_suffix):
            log.warning(f"[{self.agent_id}] [RATE_LIMIT] 消息被限流: {topic_suffix}")
            return None

        topic = f"{self._prefix}{topic_suffix}"
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, ensure_ascii=False)
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        if qos is None:
            # 从 topic 后缀猜测 QoS
            for key, val in config.QOS.items():
                if key in topic_suffix:
                    qos = val
                    break
            if qos is None:
                qos = 1

        # MQTT 5.0 Properties
        pub_props = None
        if properties and self.mqtt_version == 5:
            pub_props = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
            if "message_expiry" in properties:
                pub_props.MessageExpiryInterval = properties["message_expiry"]
            if "user_properties" in properties:
                pub_props.UserProperty = properties["user_properties"]

        info = self._client.publish(topic, payload, qos=qos, retain=retain, properties=pub_props)

    def publish_stream(self, topic_suffix: str, seq: int, data: str):
        """发布流式消息（stdout/stderr），QoS=0，不 Retain"""
        payload = json.dumps({"seq": seq, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "data": data}, ensure_ascii=False)
        self.publish(topic_suffix, payload, retain=False, qos=0)

    # ── 订阅 ──

    def subscribe(self, topic_suffix: str, callback: Callable, qos: int = 1):
        """
        订阅主题并在收到消息时调用 callback(msg)。

        callback 接收参数: (topic: str, payload: dict|str|bytes)
        """
        # 先注册回调，再订阅（避免 retain 消息在回调注册前到达）
        if topic_suffix not in self._subscriptions:
            self._subscriptions[topic_suffix] = []
        self._subscriptions[topic_suffix].append(callback)
        # v2/ 主题已是完整路径，不加 agent/ 前缀
        if topic_suffix.startswith("v2/") or topic_suffix.startswith("board/"):
            topic = topic_suffix
        else:
            topic = f"{self._prefix}{topic_suffix}"
        self._client.subscribe(topic, qos)
        log.info(f"[SUB] {topic}")
        return self

    def unsubscribe(self, topic_suffix: str):
        """取消订阅"""
        topic = f"{self._prefix}{topic_suffix}"
        self._client.unsubscribe(topic)
        self._subscriptions.pop(topic_suffix, None)

    # ── 发布+订阅（认领模式，对标 subagent 启动） ──

    def publish_and_subscribe(self, pub_topic: str, payload: Any, sub_topic: str, callback: Callable,
                               retain: bool = True, qos: Optional[int] = None):
        """先发布一条消息（如 claim），再订阅一个主题（如 signal）"""
        self.publish(pub_topic, payload, retain=retain, qos=qos)
        self.subscribe(sub_topic, callback)

    # ── 回调处理 ──

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        try:
            if self.mqtt_version == 5:
                # MQTT 5.0: rc 是 ReasonCode 对象
                self._connected = (rc.value == 0)
            else:
                self._connected = (rc == 0)
            if self._connected:
                self.publish(f"node/{self.agent_id}/status", "online", retain=True)
                log.info(f"[{self.agent_id}] 已连接 (rc={rc})")
            else:
                log.warning(f"[{self.agent_id}] 连接失败 (rc={rc})")
        except Exception as e:
            log.error(f"[{self.agent_id}] _on_connect 异常: {e}")
            import traceback; traceback.print_exc()

    def _on_disconnect(self, client, userdata, rc, properties=None, reasonCodeProperties=None):
        self._connected = False
        log.info(f"[{self.agent_id}] 断开 (rc={rc})")

    def _on_message(self, client, userdata, msg):
        """收到消息 → 匹配回调并派发"""
        topic = msg.topic
        # 去掉前缀
        suffix = topic[len(self._prefix):] if topic.startswith(self._prefix) else topic
        payload = msg.payload
        # 尝试 JSON 解析
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        # 匹配已注册的回调
        for pattern, callbacks in self._subscriptions.items():
            if self._topic_matches(pattern, suffix):
                for cb in callbacks:
                    try:
                        cb(suffix, payload)
                    except Exception as e:
                        log.error(f"回调异常 [{pattern}]: {e}")

    @staticmethod
    def _topic_matches(pattern: str, topic: str) -> bool:
        """简单的通配符匹配（支持 + 和 #）"""
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

    @property
    def is_connected(self) -> bool:
        return self._connected
