"""
MQTT BBS 审计日志模块

提供结构化审计日志记录，用于安全事件追踪、合规审计和问题排查。

审计事件类型:
    - CONNECT: 客户端连接/断开
    - AUTH: 认证事件（成功/失败）
    - PUBLISH: 消息发布
    - SUBSCRIBE: 主题订阅
    - COMMAND: 管理命令
    - SECURITY: 安全事件（签名失败/越权访问）
    - ERROR: 系统错误

用法:
    from Mqtt_bbs_client.audit_log import AuditLogger, AuditEvent

    logger = AuditLogger(client)
    logger.log(AuditEvent(type="AUTH", detail="login success", agent_id="agent_alpha"))

    # 快捷方法
    logger.auth_success("agent_alpha", "login")
    logger.security_warning("Plugin signature verification failed", plugin="my_plugin")

集成到 BBSClient:
    client = BBSClient("agent_alpha")
    # client.audit_logger.auth_success(client.agent_id, "connect")
"""

import json
import time
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, Any

log = logging.getLogger("Mqtt_bbs.audit")


@dataclass
class AuditEvent:
    """审计事件结构体"""
    type: str          # CONNECT, AUTH, PUBLISH, SUBSCRIBE, COMMAND, SECURITY, ERROR
    detail: str        # 事件描述
    severity: str = "INFO"      # INFO, WARNING, ERROR, CRITICAL
    result: str = "SUCCESS"     # SUCCESS, FAILURE, BLOCKED
    agent_id: str = ""
    topic: str = ""
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


class AuditLogger:
    """审计日志记录器

    支持:
    - 本地日志文件输出
    - MQTT 主题发布 (发送到 system/audit/log 供监控服务消费)
    - 可启用/禁用
    """

    def __init__(
        self,
        mqtt_client=None,
        log_topic: str = "system/audit/log",
        enabled: bool = True,
        log_to_console: bool = True,
    ):
        self._mqtt_client = mqtt_client
        self._log_topic = log_topic
        self._enabled = enabled
        self._log_to_console = log_to_console

    def log(self, event: AuditEvent) -> bool:
        """记录审计事件

        返回是否成功记录
        """
        if not self._enabled:
            return False

        try:
            data = event.to_dict()

            # 1. 控制台日志
            if self._log_to_console:
                prefix = f"[AUDIT][{event.severity}] {event.type}"
                log.info(f"{prefix}: {event.detail} (agent={event.agent_id}, result={event.result})")

            # 2. MQTT 发布 (如果提供了 client)
            if self._mqtt_client and hasattr(self._mqtt_client, 'publish'):
                try:
                    self._mqtt_client.publish(
                        self._log_topic,
                        data,
                        qos=1,
                        retain=False,
                        bypass_rate_limit=True,
                    )
                except Exception as e:
                    log.warning(f"[AUDIT] MQTT publish failed: {e}")

            return True
        except Exception as e:
            log.error(f"[AUDIT] Log failed: {e}")
            return False

    # ── 快捷方法 ──

    def auth_success(self, agent_id: str, method: str, **meta):
        """记录认证成功"""
        return self.log(AuditEvent(
            type="AUTH", severity="INFO", result="SUCCESS",
            detail=f"Authentication success ({method})",
            agent_id=agent_id, metadata=meta,
        ))

    def auth_failure(self, agent_id: str, method: str, reason: str, **meta):
        """记录认证失败"""
        return self.log(AuditEvent(
            type="AUTH", severity="WARNING", result="FAILURE",
            detail=f"Authentication failure ({method}): {reason}",
            agent_id=agent_id, metadata=meta,
        ))

    def connect(self, agent_id: str, **meta):
        """记录连接事件"""
        return self.log(AuditEvent(
            type="CONNECT", severity="INFO", result="SUCCESS",
            detail="Client connected",
            agent_id=agent_id, metadata=meta,
        ))

    def disconnect(self, agent_id: str, **meta):
        """记录断开事件"""
        return self.log(AuditEvent(
            type="CONNECT", severity="INFO", result="SUCCESS",
            detail="Client disconnected",
            agent_id=agent_id, metadata=meta,
        ))

    def security_warning(self, detail: str, **meta):
        """记录安全警告"""
        return self.log(AuditEvent(
            type="SECURITY", severity="WARNING", result="BLOCKED",
            detail=detail, metadata=meta,
        ))

    def security_critical(self, detail: str, **meta):
        """记录严重安全事件"""
        return self.log(AuditEvent(
            type="SECURITY", severity="CRITICAL", result="BLOCKED",
            detail=detail, metadata=meta,
        ))

    def command(self, agent_id: str, action: str, **meta):
        """记录管理命令"""
        return self.log(AuditEvent(
            type="COMMAND", severity="INFO", result="SUCCESS",
            detail=f"Command: {action}", metadata=meta,
        ))

    def system_error(self, detail: str, **meta):
        """记录系统错误"""
        return self.log(AuditEvent(
            type="ERROR", severity="ERROR", result="FAILURE",
            detail=detail, metadata=meta,
        ))

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        self._enabled = value
