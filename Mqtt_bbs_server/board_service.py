"""
Board Service — 向后兼容包装层

从原始 940 行 board_service.py 重构为多模块架构：
  - board_config.py:  配置与常量 (58 行)
  - board_db.py:      数据库与能力注册表 (180 行)
  - board_handlers.py: MQTT 消息处理器 (530 行)
  - board_core.py:    核心生命周期与路由 (260 行)

向后兼容: 所有 from Mqtt_bbs.board_service import BoardService 继续可用。
"""

from .board_core import BoardService, main

__all__ = ["BoardService", "main"]
