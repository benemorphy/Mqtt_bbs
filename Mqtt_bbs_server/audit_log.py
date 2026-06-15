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
    from audit_log import AuditLogger, AuditEvent

    logger = AuditLogger(client)
    logger.log(AuditEvent(type="AUTH", detail="login success", agent_id="agent_alpha"))
"""

import json
import time
import logging
from typing import Optional
from dataclasses import dataclass, field, asdict


@dataclass
class AuditEvent:
    """单个审计事件的数据结构"""
    type: str                    # 事件类型: CONNECT|AUTH|PUBLISH|SUBSCRIBE|COMMAND|SECURITY|ERROR
    detail: str                  # 事件详情描述
    agent_id: str = ""           # 相关 Agent ID
    topic: str = ""              # 相关 MQTT 主题
    severity: str = "INFO"       # 严重级别: DEBUG|INFO|WARNING|ERROR|CRITICAL
    source_ip: str = ""          # 来源 IP（如有）
    result: str = "SUCCESS"      # 结果: SUCCESS|FAILURE|DENIED
    metadata: dict = field(default_factory=dict)  # 额外元数据
    timestamp: float = field(default_factory=time.time)


class AuditLogger:
    """审计日志记录器

    支持三种输出模式:
        1. MQTT主题推送 - 发布到 AUDIT_LOG_TOPIC
        2. 本地文件写入 - 追加到 audit.log
        3. Python logging - 通过标准 logging 模块
    """

    def __init__(
        self,
        mqtt_client=None,
        log_topic: str = "system/audit/log",
        file_path: str = "",
        enabled: bool = True,
    ):
        """
        Args:
            mqtt_client: BBSClient 实例（用于发布审计事件到 MQTT）
            log_topic: 审计事件发布的 MQTT 主题
            file_path: 审计日志文件路径，为空则不写文件
            enabled: 是否启用审计
        """
        self._client = mqtt_client
        self._topic = log_topic
        self._file_path = file_path
        self._enabled = enabled
        self._log = logging.getLogger("Mqtt_bbs.audit")

    def log(self, event: AuditEvent, publish: bool = True):
        """记录一条审计事件

        Args:
            event: 审计事件
            publish: 是否发布到 MQTT
        """
        if not self._enabled:
            return

        data = asdict(event)
        data["_v"] = 1  # 格式版本

        # 1. Python logging
        level = getattr(logging, event.severity, logging.INFO)
        self._log.log(level, f"[AUDIT][{event.type}] {event.detail}")

        # 2. 文件写入
        if self._file_path:
            try:
                line = json.dumps(data, ensure_ascii=False) + "\n"
                with open(self._file_path, "a", encoding="utf-8") as f:
                    f.write(line)
            except Exception as e:
                self._log.warning(f"审计日志写入失败: {e}")

        # 3. MQTT 发布
        if publish and self._client and self._client.is_connected:
            try:
                self._client.publish(self._topic, data, qos=1)
            except Exception as e:
                self._log.warning(f"审计日志MQTT发布失败: {e}")

    # ── 便捷方法 ──

    def auth_success(self, agent_id: str, detail: str = "", **meta):
        return self.log(AuditEvent(
            type="AUTH", severity="INFO", result="SUCCESS",
            agent_id=agent_id, detail=detail or "认证成功", metadata=meta,
        ))

    def auth_failure(self, agent_id: str, detail: str = "", **meta):
        return self.log(AuditEvent(
            type="AUTH", severity="WARNING", result="FAILURE",
            agent_id=agent_id, detail=detail or "认证失败", metadata=meta,
        ))

    def security_warning(self, agent_id: str, detail: str, **meta):
        return self.log(AuditEvent(
            type="SECURITY", severity="WARNING", result="DENIED",
            agent_id=agent_id, detail=detail, metadata=meta,
        ))

    def command(self, agent_id: str, action: str, result: str = "SUCCESS", **meta):
        return self.log(AuditEvent(
            type="COMMAND", severity="INFO", result=result,
            agent_id=agent_id, detail=f"执行命令: {action}", metadata=meta,
        ))

    def system_error(self, detail: str, **meta):
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
