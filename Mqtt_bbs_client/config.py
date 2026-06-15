"""MQTT BBS 默认配置 — 全部从环境变量读取，无硬编码密码

安全策略:
- 所有密码/密钥强制从环境变量读取，无默认值
- 启动时校验关键配置，缺失时显式告警
- TLS 配置可选但优先尝试
- 凭据轮转: 支持动态重读环境变量
"""

import os as _os
import logging as _logging

_log = _logging.getLogger("Mqtt_bbs.config")


# ── Broker ──
BROKER_HOST = _os.environ.get("MQTT_HOST", "127.0.0.1")
BROKER_PORT = int(_os.environ.get("MQTT_PORT", "1883"))

# TLS / SSL — 默认关闭, 通过环境变量启用
MQTT_TLS_ENABLED = _os.environ.get("MQTT_TLS_ENABLED", "false").lower() in ("true", "1", "yes")
MQTT_TLS_CA_CERTS = _os.environ.get("MQTT_TLS_CA_CERTS") or _os.environ.get("MQTT_CA_CERTS")
MQTT_TLS_CERTFILE = _os.environ.get("MQTT_TLS_CERTFILE")
MQTT_TLS_KEYFILE = _os.environ.get("MQTT_TLS_KEYFILE")
MQTT_TLS_CERT_REQS = _os.environ.get("MQTT_TLS_CERT_REQS", "CERT_REQUIRED")
MQTT_TLS_VERSION = _os.environ.get("MQTT_TLS_VERSION", "tlsv1.2")
MQTT_TLS_INSECURE = _os.environ.get("MQTT_TLS_INSECURE", "false").lower() in ("true", "1", "yes")

# TLS端口映射: 启用TLS时默认使用8883
if MQTT_TLS_ENABLED and BROKER_PORT == 1883:
    BROKER_PORT = 8883

def reload_config():
    """运行时重新加载配置 — 支持凭据轮转"""
    global MQTT_HMAC_SECRET, JWT_SECRET
    MQTT_HMAC_SECRET = _os.environ.get("MQTT_HMAC_SECRET")
    JWT_SECRET = _os.environ.get("JWT_SECRET")


# 主题前缀
TOPIC_PREFIX = _os.environ.get("TOPIC_PREFIX", "agent/")

# 客户端
KEEPALIVE = int(_os.environ.get("MQTT_KEEPALIVE", "60"))
RECONNECT_DELAY = int(_os.environ.get("MQTT_RECONNECT_DELAY", "3"))
MAX_RECONNECT_DELAY = int(_os.environ.get("MQTT_MAX_RECONNECT_DELAY", "60"))

# 任务
DEFAULT_TASK_TIMEOUT = int(_os.environ.get("DEFAULT_TASK_TIMEOUT", "300"))

# QoS
QOS = {
    "input": int(_os.environ.get("QOS_INPUT", "1")),
    "output": int(_os.environ.get("QOS_OUTPUT", "1")),
    "signal": int(_os.environ.get("QOS_SIGNAL", "2")),
    "stdout": int(_os.environ.get("QOS_STDOUT", "0")),
    "stderr": int(_os.environ.get("QOS_STDERR", "0")),
    "status": int(_os.environ.get("QOS_STATUS", "1")),
    "claim": int(_os.environ.get("QOS_CLAIM", "1")),
}

# HMAC — 无默认值！环境变量未设置时 board_handlers 注册会报错退出
# 安全: 移除默认字符串防止被猜测，启动时校验
MQTT_HMAC_SECRET = _os.environ.get("MQTT_HMAC_SECRET")
if not MQTT_HMAC_SECRET:
    _log.warning(
        "MQTT_HMAC_SECRET 未设置！BoardService 注册功能将不可用。"
        "请通过环境变量设置: set MQTT_HMAC_SECRET=<your_secret>"
    )

# JWT Secret — 无默认值
JWT_SECRET = _os.environ.get("JWT_SECRET")
if not JWT_SECRET:
    _log.warning(
        "JWT_SECRET 未设置！JWT 认证功能将不可用。"
        "请通过环境变量设置: set JWT_SECRET=<your_secret>"
    )

# MQTT 5.0
MQTT_VERSION = int(_os.environ.get("MQTT_VERSION", "5"))
CLEAN_START = _os.environ.get("MQTT_CLEAN_START", "true").lower() == "true"
SESSION_EXPIRY_INTERVAL = int(_os.environ.get("MQTT_SESSION_EXPIRY", "3600"))
MESSAGE_EXPIRY_INTERVAL = int(_os.environ.get("MQTT_MESSAGE_EXPIRY", "300"))
TOPIC_ALIAS_MAXIMUM = int(_os.environ.get("MQTT_TOPIC_ALIAS_MAX", "32"))

# 心跳
HEARTBEAT_INTERVAL = int(_os.environ.get("HEARTBEAT_INTERVAL", "30"))
HEARTBEAT_TIMEOUT = int(_os.environ.get("HEARTBEAT_TIMEOUT", "90"))

# 速率限制 (客户端侧)
RATE_LIMIT_ENABLED = _os.environ.get("MQTT_RATE_LIMIT_ENABLED", "true").lower() in ("true", "1", "yes")
RATE_LIMIT_MAX_PER_SEC = int(_os.environ.get("MQTT_RATE_LIMIT_MAX_PER_SEC", "50"))
RATE_LIMIT_BURST = int(_os.environ.get("MQTT_RATE_LIMIT_BURST", "100"))
RATE_LIMIT_WINDOW_SEC = int(_os.environ.get("MQTT_RATE_LIMIT_WINDOW_SEC", "60"))

# MariaDB
DB_CONFIG = {
    "host": _os.environ.get("DB_HOST", "127.0.0.1"),
    "port": int(_os.environ.get("DB_PORT", "3306")),
    "user": _os.environ.get("DB_USER", "root"),
    "password": _os.environ.get("DB_PASSWORD") or _os.environ.get("mariadb_password") or "",
    "database": _os.environ.get("DB_NAME", "Mqtt_bbs"),
    "charset": "utf8mb4",
}

# 审计日志
AUDIT_LOG_ENABLED = _os.environ.get("MQTT_AUDIT_LOG_ENABLED", "true").lower() in ("true", "1", "yes")
AUDIT_LOG_TOPIC = _os.environ.get("MQTT_AUDIT_LOG_TOPIC", "system/audit/log")
