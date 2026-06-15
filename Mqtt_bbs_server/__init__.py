"""
MQTT BBS Server — Agent协作消息总线服务端

包含 BoardService、AgentBoard、WorkerAgent、BoardClient 等服务端组件。

依赖:
    Mqtt_bbs_client (客户端库)
"""

from .bbs import AgentBoard, WorkerAgent, TaskStatus
from .board_service import BoardService
from Mqtt_bbs_client.board_client import BoardClient
from .persistence import (
    BBSClientWithPersistence, MariaDBConn,
    AgentBoardWithPersistence, WorkerAgentWithPersistence
)
from .scheduler import BBScheduler
from .plugin_manager import PluginManager, FilterChain
from .dag import DAGWorkflow, DAGTask

__version__ = "0.1.0"
