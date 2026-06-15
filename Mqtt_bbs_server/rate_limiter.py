"""
MQTT 客户端速率限制器 — 令牌桶算法

防止单个 Agent 突发大量消息导致 Broker 过载。

用法:
    limiter = RateLimiter(max_per_sec=50, burst=100)
    if limiter.allow():
        client.publish(topic, payload)

支持按主题前缀限流:
    limiter = RateLimiter()
    if limiter.allow(topic="bbs/agent-bbs-test/post"):
        ...
"""

import time
import threading
from collections import defaultdict
from typing import Optional


class TokenBucket:
    """令牌桶 — 单 key 限流"""

    def __init__(self, max_per_sec: float, burst: int = 0):
        self.rate = max_per_sec
        self.burst = burst if burst > 0 else int(max_per_sec * 2)
        self._tokens = float(self.burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def consume(self, tokens: float = 1.0) -> bool:
        """消费令牌，返回是否允许"""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens


class RateLimiter:
    """基于事件循环的多粒度速率限制器

    支持全局和按主题前缀两种限流模式。
    """

    def __init__(
        self,
        max_per_sec: int = 50,
        burst: int = 100,
        enabled: bool = True,
        topic_limits: Optional[dict[str, int]] = None,
    ):
        """
        Args:
            max_per_sec: 全局每秒最大消息数
            burst: 全局突发大小
            enabled: 是否启用限流
            topic_limits: 按主题前缀限流，如 {"bbs/agent-bbs-test/post": 10}
        """
        self._enabled = enabled
        self._global_bucket = TokenBucket(max_per_sec, burst)
        self._topic_buckets: dict[str, TokenBucket] = {}
        self._topic_limits = topic_limits or {}
        self._stats_lock = threading.Lock()
        self._stats = {
            "allowed": 0,
            "denied": 0,
            "total": 0,
        }

        for topic_prefix, limit in self._topic_limits.items():
            self._topic_buckets[topic_prefix] = TokenBucket(limit, int(limit * 2))

    def allow(self, topic: str = "", tokens: float = 1.0) -> bool:
        """检查是否允许发送

        Args:
            topic: MQTT 主题（可选，用于按主题限流）
            tokens: 消耗的令牌数

        Returns:
            True=允许发送, False=被限流
        """
        if not self._enabled:
            return True

        with self._stats_lock:
            self._stats["total"] += 1

        # 检查全局令牌桶
        if not self._global_bucket.consume(tokens):
            with self._stats_lock:
                self._stats["denied"] += 1
            return False

        # 检查按主题前缀的令牌桶
        if topic:
            for prefix, bucket in self._topic_buckets.items():
                if topic.startswith(prefix):
                    if not bucket.consume(tokens):
                        with self._stats_lock:
                            self._stats["denied"] += 1
                        return False
                    break

        with self._stats_lock:
            self._stats["allowed"] += 1
        return True

    def reset(self):
        """重置所有令牌桶"""
        self._global_bucket = TokenBucket(
            self._global_bucket.rate, self._global_bucket.burst
        )
        for prefix in list(self._topic_buckets.keys()):
            limit = self._topic_limits.get(prefix, 50)
            self._topic_buckets[prefix] = TokenBucket(limit, int(limit * 2))
        with self._stats_lock:
            self._stats = {"allowed": 0, "denied": 0, "total": 0}

    def stats(self) -> dict:
        """获取限流统计"""
        with self._stats_lock:
            allowed = self._stats["allowed"]
            denied = self._stats["denied"]
            total = self._stats["total"]
        return {
            "allowed": allowed,
            "denied": denied,
            "total": total,
            "block_rate": round(denied / max(total, 1) * 100, 2),
            "global_available": round(self._global_bucket.available, 1),
        }

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        self._enabled = value
