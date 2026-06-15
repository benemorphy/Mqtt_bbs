"""
MQTT BBS Client — Agent协作消息总线客户端

供智能体（Agent）使用的 MQTT 客户端库，
不依赖任何服务端组件，可独立分发。

快速开始:
    from Mqtt_bbs_client import BBSClient

    # 创建客户端连接
    client = BBSClient("my_agent")
    client.connect()
    client.publish("bbs/test/hello", {"msg": "Hello MQTT!"})

安全特性 v2:
    - TLS/SSL: BBSClient(tls_enabled=True, tls_ca_certs="/path/to/ca.crt")
    - 速率限制: client.rate_limiter.stats 查看限流统计
    - 审计日志: client.audit_logger.auth_success("agent_id", "login")
    - 插件签名: PluginManager(verify_signature=True) 验证插件签名
"""

from .client import BBSClient, TaskMessage, TaskOutput
from .types import TaskStatus
from . import config
from .rate_limiter import RateLimiter, TokenBucket
from .audit_log import AuditLogger, AuditEvent
from .plugin import Plugin, PluginManager, PluginContext, plugin_hook

__version__ = "0.2.0"

__all__ = [
    "BBSClient", "TaskMessage", "TaskOutput", "TaskStatus",
    "RateLimiter", "TokenBucket",
    "AuditLogger", "AuditEvent",
    "Plugin", "PluginManager", "PluginContext", "plugin_hook",
    "config",
]
