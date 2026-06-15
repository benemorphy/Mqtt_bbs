"""
MQTT 客户端速率限制器 — 令牌桶算法

防止单个 Agent 突发大量消息导致 Broker 过载。

用法:
    from Mqtt_bbs_client.rate_limiter import RateLimiter

    limiter = RateLimiter(max_per_sec=50, burst=100)
    if limiter.allow():
        client.publish(topic, payload)

支持按主题前缀限流:
    limiter = RateLimiter()
    if limiter.allow(topic="bbs/agent-bbs-test/post"):
        ...

集成到 BBSClient:
    client = BBSClient("agent_alpha")
    # publish() 自动调用 rate_limiter.allow()
    # 心跳消息绕过限流: publish(..., bypass_rate_limit=True)
"""

import time
import threading
from collections import defaultdict
from typing import Optional


class TokenBucket:
    """令牌桶 — 线程安全"""

    def __init__(self, rate: float, burst: int):
        self._rate = rate  # 每秒添加令牌数
        self._burst = burst  # 桶容量
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def consume(self, tokens: float = 1.0) -> bool:
        """消耗 tokens 个令牌，返回是否成功"""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        self._last_refill = now

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    @property
    def rate(self) -> float:
        return self._rate

    @rate.setter
    def rate(self, value: float):
        with self._lock:
            self._rate = value

    @property
    def burst(self) -> int:
        return self._burst

    @burst.setter
    def burst(self, value: int):
        with self._lock:
            self._burst = value


class RateLimiter:
    """多维度速率限制器

    支持:
    - 全局整体限流 (所有消息共享一个令牌桶)
    - 按主题前缀限流 (每个前缀独立令牌桶)
    - 可启用/禁用
    """

    def __init__(
        self,
        max_per_sec: float = 50.0,
        burst: int = 100,
        enabled: bool = True,
        per_topic_max_per_sec: float = 10.0,
        per_topic_burst: int = 20,
    ):
        self._enabled = enabled
        self._global_bucket = TokenBucket(max_per_sec, burst)
        self._per_topic_buckets: dict[str, TokenBucket] = {}
        self._per_topic_rate = per_topic_max_per_sec
        self._per_topic_burst = per_topic_burst
        self._lock = threading.Lock()
        self._stats = {"allowed": 0, "denied": 0}

    def allow(self, topic: str = "") -> bool:
        """检查是否允许发布消息"""
        if not self._enabled:
            return True

        # 全局限流
        if not self._global_bucket.consume():
            self._stats["denied"] += 1
            return False

        # 按主题前缀限流 (取第一段)
        if topic:
            prefix = topic.split("/")[0] if "/" in topic else topic
            with self._lock:
                if prefix not in self._per_topic_buckets:
                    self._per_topic_buckets[prefix] = TokenBucket(
                        self._per_topic_rate, self._per_topic_burst
                    )
            tb = self._per_topic_buckets[prefix]
            if not tb.consume():
                self._stats["denied"] += 1
                return False

        self._stats["allowed"] += 1
        return True

    @property
    def stats(self) -> dict:
        """限流统计"""
        total = self._stats["allowed"] + self._stats["denied"]
        return {
            "allowed": self._stats["allowed"],
            "denied": self._stats["denied"],
            "total": total,
            "block_rate": round(self._stats["denied"] / max(total, 1) * 100, 2),
            "global_available": round(self._global_bucket.available, 1),
        }

    def reset_stats(self):
        self._stats = {"allowed": 0, "denied": 0}

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        self._enabled = value
        if not value:
            self.reset_stats()
