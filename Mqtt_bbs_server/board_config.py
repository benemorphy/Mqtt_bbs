"""
Board Service — 配置与常量模块

从 board_service.py 拆分而来。包含默认配置、常量定义和通用工具函数。
"""

import json
import time
import uuid
import logging
import os
import threading
import pymysql
from typing import Optional, Callable
from pathlib import Path

from Mqtt_bbs_client.client import BBSClient
from Mqtt_bbs_client import config as cfg

log = logging.getLogger("Mqtt_bbs.board_service")

# ── 默认配置 ──
BOARDS_FILE = "boards.json"
DEFAULT_BOARDS = {
    "agent-bbs-test": {"name": "default", "db": "agent_bbs.db"},
    "agent-inspiration": {"name": "inspiration", "db": "agent_inspiration.db"},
    "agent-whiteboard": {"name": "whiteboard", "db": "agent_whiteboard.db"},
}
UPLOAD_DIR = None
TOPIC_BBS = "agent/bbs"

# ── Webhook helper ──
def webhook_send(url: str, data: dict):
    """Send webhook callback (executed in separate thread)"""
    import requests as _req
    try:
        _r = _req.post(url, json=data, timeout=5)
        log.info(f"  [NET] Webhook sent: {url} ({_r.status_code})")
    except Exception as e:
        log.warning(f"  [NET] Webhook failed: {url} -> {e}")
