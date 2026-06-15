"""
MQTT BBS 共享数据类型 — 无依赖，可被 client/server 双方安全引用

从 client.py 提取: TaskMessage, TaskOutput
从 bbs.py 提取: TaskStatus
"""

from typing import Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import time


# ──────────────────────────────────────────────
# 任务状态枚举
# ──────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ──────────────────────────────────────────────
# 任务消息（对标 input.txt 的 JSON 内容）
# ──────────────────────────────────────────────

@dataclass
class TaskMessage:
    """任务消息（对标 input.txt 的 JSON 内容）"""
    task_id: str
    type: str
    input: dict
    priority: int = 3
    timeout: int = 300
    created_at: str = ""
    resources: list = field(default_factory=list)

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "type": self.type,
            "input": self.input,
            "priority": self.priority,
            "timeout": self.timeout,
            "created_at": self.created_at or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "resources": self.resources,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ──────────────────────────────────────────────
# 任务输出（对标 output.txt）
# ──────────────────────────────────────────────

@dataclass
class TaskOutput:
    """任务输出（对标 output.txt）"""
    task_id: str
    agent_id: str
    status: str  # completed | failed
    result: Any = None
    error: Optional[dict] = None
    metrics: dict = field(default_factory=dict)

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "status": self.status,
            "result": self.result,
            "error": self.error,
            "metrics": self.metrics,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})
