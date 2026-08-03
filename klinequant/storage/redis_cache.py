"""RedisCacheManager — Redis 缓存管理器

KlineQuant 缓存层，用于：
    - K 线最新快照（最新收盘 K 线 + 当前未完成 K 线）
    - 持仓快照
    - 账户余额快照
    - 信号缓存
    - 通用 KV 缓存

遵循技术文档 §3.3 Redis 缓存规范。
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


# Decimal JSON 序列化/反序列化
def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _json_loads(data: str) -> Any:
    """JSON 反序列化（Decimal 保留为字符串，由调用方按需转换）"""
    return json.loads(data)


class RedisCacheManager:
    """Redis 异步缓存管理器。

    用法：
        cache = RedisCacheManager()
        await cache.initialize()

        # KV 操作
        await cache.set("kline:BTCUSDT:1m:latest", {"close": 60000}, ttl=60)
        data = await cache.get("kline:BTCUSDT:1m:latest")

        # Hash 操作（适合结构化缓存如持仓/账户）
        await cache.hset("positions:strategy_001", "BTCUSDT", {"qty": 1.5})
        pos = await cache.hget("positions:strategy_001", "BTCUSDT")

        # 批量
        await cache.delete("kline:BTCUSDT:1m:latest", "kline:ETHUSDT:1m:latest")

        await cache.close()
    """

    # 默认 TTL（秒）
    DEFAULT_TTL = 300       # 5 分钟
    KLINE_TTL = 120         # K 线快照 2 分钟
    POSITION_TTL = 60       # 持仓快照 1 分钟
    ACCOUNT_TTL = 30        # 账户快照 30 秒

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        key_prefix: str = "kq:",
    ):
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._key_prefix = key_prefix
        self._redis: Optional[aioredis.Redis] = None

    async def initialize(self) -> None:
        """初始化 Redis 连接"""
        self._redis = aioredis.Redis(
            host=self._host,
            port=self._port,
            db=self._db,
            password=self._password,
            decode_responses=True,
        )
        # 测试连接
        await self._redis.ping()
        logger.info(f"Redis connected: {self._host}:{self._port}/{self._db}")

    async def close(self) -> None:
        """关闭 Redis 连接"""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            logger.info("Redis closed")

    def _key(self, key: str) -> str:
        """添加前缀"""
        return f"{self._key_prefix}{key}"

    # ─── KV 操作 ───

    async def get(self, key: str) -> Optional[Any]:
        """获取缓存值（自动 JSON 反序列化）"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        data = await self._redis.get(self._key(key))
        if data is None:
            return None
        return _json_loads(data)

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存值（自动 JSON 序列化 + TTL）"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        data = json.dumps(value, default=_json_default)
        k = self._key(key)
        if ttl:
            await self._redis.set(k, data, ex=ttl)
        else:
            await self._redis.set(k, data)

    async def delete(self, *keys: str) -> int:
        """删除一个或多个缓存键"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        prefixed = [self._key(k) for k in keys]
        return await self._redis.delete(*prefixed)

    async def exists(self, key: str) -> bool:
        """检查键是否存在"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        return bool(await self._redis.exists(self._key(key)))

    async def ttl(self, key: str) -> int:
        """获取键的剩余 TTL（秒），-1 为永不过期，-2 为不存在"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        return await self._redis.ttl(self._key(key))

    # ─── Hash 操作 ───

    async def hget(self, name: str, key: str) -> Optional[Any]:
        """获取 Hash 字段值"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        data = await self._redis.hget(self._key(name), key)
        if data is None:
            return None
        return _json_loads(data)

    async def hset(self, name: str, key: str, value: Any) -> None:
        """设置 Hash 字段值"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        data = json.dumps(value, default=_json_default)
        await self._redis.hset(self._key(name), key, data)

    async def hgetall(self, name: str) -> Dict[str, Any]:
        """获取整个 Hash"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        raw = await self._redis.hgetall(self._key(name))
        return {k: _json_loads(v) for k, v in raw.items()}

    async def hdel(self, name: str, *keys: str) -> int:
        """删除 Hash 字段"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        return await self._redis.hdel(self._key(name), *keys)

    # ─── 批量 / Pipeline ───

    async def mget(self, keys: List[str]) -> List[Optional[Any]]:
        """批量获取"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        prefixed = [self._key(k) for k in keys]
        results = await self._redis.mget(prefixed)
        return [_json_loads(r) if r is not None else None for r in results]

    async def mset(self, mapping: Dict[str, Any], ttl: Optional[int] = None) -> None:
        """批量设置（使用 pipeline 提高效率）"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        pipe = self._redis.pipeline()
        for key, value in mapping.items():
            k = self._key(key)
            data = json.dumps(value, default=_json_default)
            if ttl:
                pipe.set(k, data, ex=ttl)
            else:
                pipe.set(k, data)
        await pipe.execute()

    # ─── 通配符删除 ───

    async def delete_pattern(self, pattern: str) -> int:
        """删除匹配模式的所有键（谨慎使用）"""
        if not self._redis:
            raise RuntimeError("Redis not initialized")
        full_pattern = self._key(pattern)
        keys = []
        async for key in self._redis.scan_iter(match=full_pattern, count=100):
            keys.append(key)
        if keys:
            return await self._redis.delete(*keys)
        return 0
