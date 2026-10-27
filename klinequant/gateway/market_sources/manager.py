"""市场源注册表 + 订阅路由 + 统一广播

职责：
    - register/get/list：插件注册与查询
    - start：后台分发循环——按各插件的当前订阅集启动/重启其 stream_loop
      （订阅集合变化触发重启，沿用币安链路已修复的周期检测模式）
    - publish_bar：去重签名后广播（原 ws_kline._publish_bar 迁移为共用）
    - active_targets：供插件读取当前订阅集

WS 主题新格式 klines.{exchange}.{symbol}.{tf}，同时兼容广播旧格式
klines.{symbol}.{tf} / klines.{symbol}；exchange 维度另由 payload 的
exchange 字段与 REST 参数承载。
"""
from __future__ import annotations

import asyncio
import logging
import time

from klinequant.gateway.market_sources.base import MarketSource
from klinequant.gateway.market_sources.derived import (
    base_tf_for,
    bucket_label,
    is_derived,
    parse_tf,
)
from klinequant.gateway.ws import ws_manager

logger = logging.getLogger(__name__)

VALID_TIMEFRAMES = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "3d", "1w"}
# 派生聚合的基础周期（实时流驱动网关聚合的入口档：日内倍率←1m，日级及以上←1d）
_BASE_CANDIDATES = frozenset({"1m", "1d"})
# 订阅扫描节奏：订阅集合变化最多延迟该秒数反映到插件重连
_TARGET_POLL_INTERVAL = 1.0
# 全量品种目录缓存（秒）：终端/交易所目录极少变化，避免每次打开搜索弹窗重新枚举
SYMBOLS_CACHE_TTL = 1800.0
# 未就绪源后台补注册复查间隔（秒）：源自身 _try_reconnect 带冷却护栏，此处仅控制复查节奏
_PENDING_RETRY_INTERVAL = 15.0


class MarketSourceManager:
    """插件注册表 + 订阅分发"""

    def __init__(self) -> None:
        self._sources: dict[str, MarketSource] = {}
        # 去重签名：f"{exchange}_{symbol}_{tf}" -> sig
        self._last_bar: dict[str, str] = {}
        # 派生周期实时聚合状态：f"{exchange}_{symbol}_{tf}" -> 当前桶累积
        # {label, open, high, low, close, base_vol(已完结日量), day_ts, day_vol(当日累计量)}
        self._derived_state: dict[str, dict] = {}
        # 全量品种目录缓存：exchange -> (monotonic ts, rows)
        self._symbols_cache: dict[str, tuple[float, list[dict]]] = {}
        self._started = False
        self._tasks: list[asyncio.Task] = []
        # 启动时未就绪的源（终端/行情服务器未连接）：由 _pending_loop 后台重试补注册，
        # 就绪后自动上线，无需重启网关（启动竞态根治）
        self._pending: list[MarketSource] = []

    # ─── 注册与查询 ───

    def register(self, source: MarketSource) -> None:
        self._sources[source.name] = source
        logger.info(f"Market source registered: {source.name} ({source.label})")

    def defer(self, source: MarketSource) -> None:
        """登记启动时未就绪的源：交由 _pending_loop 后台重试，就绪后自动 register"""
        self._pending.append(source)
        logger.info(
            f"Market source deferred (not ready at bootstrap): "
            f"{source.name} ({source.label})"
        )

    def get(self, exchange: str) -> MarketSource | None:
        return self._sources.get((exchange or "").lower())

    def list_sources(self) -> list[MarketSource]:
        return list(self._sources.values())

    def default_exchange(self) -> str:
        """无 exchange 参数时的默认所：优先 binance，否则取首个注册插件"""
        if "binance" in self._sources:
            return "binance"
        return next(iter(self._sources), "")

    async def list_symbols(self, exchange: str) -> list[dict] | None:
        """指定源全量品种目录（TTL 缓存）；未注册源返回 None，拉取失败向上抛出"""
        source = self.get(exchange)
        if source is None:
            return None
        cached = self._symbols_cache.get(source.name)
        if cached and time.monotonic() - cached[0] < SYMBOLS_CACHE_TTL:
            return cached[1]
        rows = await source.list_symbols()
        # 降级回退不写缓存：源重写了 list_symbols（有动态全量目录）但本次返回的却是
        # default_symbols 归一化副本（如 ths 启动早期未连上/目录接口瞬时失败）；若缓存会
        # 把降级目录锁满 TTL（30min），源就绪后仍拿不到真实目录。未重写 list_symbols 的源
        # （如 IG 静态目录）其 default_symbols 即真实目录，照常缓存
        if type(source).list_symbols is not MarketSource.list_symbols and rows == [
            {"symbol": s["symbol"], "name": s.get("name", s["symbol"]),
             "type": s.get("type", ""), "code": s.get("code", s["symbol"])}
            for s in (source.default_symbols or [])
        ]:
            return rows
        self._symbols_cache[source.name] = (time.monotonic(), rows)
        return rows

    # ─── 订阅路由 ───

    def _active_targets(self, exchange: str) -> set[tuple[str, str]]:
        """当前订阅了该所 klines 主题的 (symbol, tf) 集合（兼容旧格式主题）"""
        targets: set[tuple[str, str]] = set()
        for topic, subs in list(ws_manager._subscriptions.items()):
            if not subs or not topic.startswith("klines."):
                continue
            parts = topic.split(".")
            if len(parts) == 4 and parts[0] == "klines" and parts[1] == exchange:
                symbol, tf = parts[2], parts[3]
            elif len(parts) == 3 and parts[0] == "klines":
                # 旧格式 klines.{symbol}.{tf} 归默认所，保持旧前端兼容
                if exchange != self.default_exchange():
                    continue
                symbol, tf = parts[1], parts[2]
            else:
                continue
            # 原生/派生分流按源判定：同是 4h，币安/MT5 原生直供、ths 无供给需由 1m 派生。
            # src 未注册时退回全局 VALID_TIMEFRAMES（保持无源上下文时的既有语义）。
            src = self._sources.get(exchange)
            supported = src.supported_timeframes if src is not None else VALID_TIMEFRAMES
            if tf in supported:
                targets.add((symbol, tf))
            elif is_derived(tf):
                # 格式派生且本源无原生供给（各源 1M/1Q/1Y、日内倍率 Nm/Nh、日级倍率 Nd/Nw，
                # 及 ths 缺的 2h/4h 等）：映射为隐式基础周期订阅（1m/1d），由基础 K 实时流
                # 驱动网关聚合（源侧零改动）；基础档也须本源可供，否则无从聚合
                base = base_tf_for(tf)
                if base in supported:
                    targets.add((symbol, base))
            elif tf in VALID_TIMEFRAMES:
                # 全局原生档但本源支持表未声明且不可派生（如桩源精简支持表 / mt5 无 3d）：
                # 仍按原生流交给源侧自行处理，保持既有行为不回退
                targets.add((symbol, tf))
        return targets

    def active_targets(self, exchange: str) -> set[tuple[str, str]]:
        """供插件读取；无订阅者时回退到插件默认监控集"""
        targets = self._active_targets(exchange)
        if not targets:
            source = self._sources.get(exchange)
            if source and source.watched_targets:
                targets = set(source.watched_targets)
        return targets

    # ─── 广播 ───

    async def publish_bar(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        bar: dict,
    ) -> bool:
        """去重后广播一根 bar（各插件实时流与降级轮询共用），返回是否实际广播"""
        cache_key = f"{exchange}_{symbol}_{timeframe}"
        sig = f"{bar['timestamp']}_{bar['high']}_{bar['low']}_{bar['close']}"
        if self._last_bar.get(cache_key) == sig:
            return False
        self._last_bar[cache_key] = sig
        payload = dict(bar)
        payload["exchange"] = exchange
        await ws_manager.publish(f"klines.{exchange}.{symbol}.{timeframe}", payload)
        # 兼容旧格式订阅（无 exchange 维度的旧前端）
        await ws_manager.publish(f"klines.{symbol}.{timeframe}", payload)
        await ws_manager.publish(f"klines.{symbol}", payload)
        # IND-102：驱动指标引擎增量计算并推送 indicators.* 主题（无注册指标时零开销）
        try:
            from klinequant.gateway.indicator_service import on_bar
            await on_bar(exchange, symbol, timeframe, payload)
        except Exception as e:
            logger.debug(f"Indicator bridge error: {e}")
        # 派生周期实时聚合：基础周期（1m/1d）bar 更新时合成各订阅派生周期的最新 bar
        # （指标 on_bar 随之自动驱动；非基础档直接跳过，免每根 bar 扫描订阅表）
        if timeframe in _BASE_CANDIDATES:
            try:
                await self._feed_derived(exchange, symbol, timeframe, payload)
            except Exception as e:
                logger.debug(f"Derived aggregation error: {e}")
        return True

    # ─── 派生周期实时聚合 ───

    def _derived_subs(self, exchange: str, symbol: str, base_tf: str) -> list[str]:
        """当前订阅了该品种、且以 base_tf 为聚合基础的派生周期列表（WS 主题解析）

        按源排除原生直供档：格式可派生但源原生支持（如币安 2h）时实时 bar 由源流
        直接发布，不得再从基础流重复聚合（双口径互相覆盖）。
        """
        source = self._sources.get(exchange)
        native = source.supported_timeframes if source else set()
        out = []
        prefix = f"klines.{exchange}.{symbol}."
        for topic, subs in list(ws_manager._subscriptions.items()):
            if subs and topic.startswith(prefix):
                tf = topic[len(prefix):]
                if is_derived(tf) and tf not in native and base_tf_for(tf) == base_tf:
                    out.append(tf)
        return out

    async def _seed_derived_state(
        self, exchange: str, symbol: str, tf: str, base_bar: dict, parsed: tuple[int, str],
    ) -> dict | None:
        """冷启动/桶翻滚补齐：拉桶起始至上一根基础 K 历史预填

        防月线/日内倍率最新 bar 开盘价、量基线缺失。
        """
        source = self._sources.get(exchange)
        if source is None:
            return None
        base_tf = base_tf_for(tf)
        base_ts = int(base_bar["timestamp"])
        label = bucket_label(base_ts, parsed)
        # 预填窗口 = 桶起点至当前基础 bar 之前；+8 冗余覆盖窗口估算误差，封顶防滥用
        span_ms = max(1, base_ts - label)
        unit_ms = 60_000 if base_tf == "1m" else 86_400_000
        need = min(span_ms // unit_ms + 8, 2000)
        try:
            from klinequant.gateway.market_sources.kline_cache import cached_klines
            bars = await cached_klines(source, symbol, base_tf, limit=need, end_time=base_ts - 1)
        except Exception as e:
            logger.warning(f"Derived seed fetch failed [{exchange}] {symbol}/{tf}: {e}")
            return None
        bars = [b for b in (bars or []) if label <= int(b["timestamp"]) < base_ts]
        if not bars:
            return None   # 当前 bar 即桶首根，无需预填（直接用实时基础 K）
        return {
            "label": label,
            "open": float(bars[0]["open"]),
            "high": max(float(b["high"]) for b in bars),
            "low": min(float(b["low"]) for b in bars),
            "close": float(bars[-1]["close"]),
            "base_vol": sum(float(b.get("volume") or 0) for b in bars),
            "day_ts": None,
            "day_vol": 0.0,
        }

    async def _feed_derived(self, exchange: str, symbol: str, base_tf: str, base_bar: dict) -> None:
        """基础周期（1m/1d）实时 bar 喂入聚合器：为每个订阅的派生周期合成并发布最新 bar"""
        for tf in self._derived_subs(exchange, symbol, base_tf):
            parsed = parse_tf(tf)
            if parsed is None:
                continue
            base_ts = int(base_bar["timestamp"])
            label = bucket_label(base_ts, parsed)
            key = f"{exchange}_{symbol}_{tf}"
            st = self._derived_state.get(key)
            if st is None or st["label"] != label:
                st = await self._seed_derived_state(exchange, symbol, tf, base_bar, parsed) or {
                    "label": label,
                    "open": float(base_bar["open"]),
                    "high": float(base_bar["high"]),
                    "low": float(base_bar["low"]),
                    "close": float(base_bar["close"]),
                    "base_vol": 0.0,
                    "day_ts": None,
                    "day_vol": 0.0,
                }
                self._derived_state[key] = st
            if st["day_ts"] != base_ts:
                st["base_vol"] += st["day_vol"]   # 上一基础 bar 累计量结转，防同 bar 量重复叠加
                st["day_ts"] = base_ts
            st["day_vol"] = float(base_bar.get("volume") or 0)
            st["high"] = max(st["high"], float(base_bar["high"]))
            st["low"] = min(st["low"], float(base_bar["low"]))
            st["close"] = float(base_bar["close"])
            bar = {
                "timestamp": label,
                "open": st["open"], "high": st["high"], "low": st["low"], "close": st["close"],
                "volume": st["base_vol"] + st["day_vol"],
                "event_ms": int(time.time() * 1000),
            }
            await self.publish_bar(exchange, symbol, tf, bar)

    # ─── 启动与分发 ───

    async def start(self) -> None:
        """启动分发循环（幂等）：为每个插件维护 stream_loop 任务，订阅集变化时重启"""
        if self._started:
            return
        self._started = True
        self._tasks.append(asyncio.create_task(self._dispatch_loop()))
        if self._pending:
            self._tasks.append(asyncio.create_task(self._pending_loop()))
        logger.info(f"Market source manager started: {sorted(self._sources.keys())}")

    async def _dispatch_loop(self) -> None:
        running: dict[str, tuple[set[tuple[str, str]], asyncio.Task]] = {}
        while True:
            for ex, source in list(self._sources.items()):
                targets = self.active_targets(ex)
                entry = running.get(ex)
                if entry is None:
                    running[ex] = (targets, asyncio.create_task(source.stream_loop()))
                elif targets != entry[0]:
                    logger.info(f"[{ex}] subscription set changed, restarting stream loop")
                    entry[1].cancel()
                    running[ex] = (targets, asyncio.create_task(source.stream_loop()))
            await asyncio.sleep(_TARGET_POLL_INTERVAL)

    async def _pending_loop(self) -> None:
        """未就绪源后台补注册：终端/行情服务器就绪后自动上线，无需重启网关

        根治启动竞态——MT5 终端与网关几乎同时启动时，bootstrap 那一刻 initialize
        尚未就绪会被一次性跳过（外汇无数据 + 品种分类缺失），而 stream_loop 只对
        已注册源重连，跳过后再也补不回来。此处按节奏重试各源 _try_reconnect
        （源侧自带冷却护栏），available 转真即 register，_dispatch_loop 随后自动
        拉起其 stream_loop。
        """
        while self._pending:
            for src in list(self._pending):
                try:
                    await asyncio.to_thread(src._try_reconnect)
                except Exception as e:
                    logger.debug(f"Pending source reconnect error [{src.name}]: {e}")
                    continue
                if getattr(src, "available", False):
                    self._pending.remove(src)
                    self.register(src)
            if self._pending:
                await asyncio.sleep(_PENDING_RETRY_INTERVAL)
        logger.info("All deferred market sources online")


# 全局单例
market_manager = MarketSourceManager()


def bootstrap_sources() -> None:
    """按 KQ_MARKET_SOURCES（默认全部）注册启用的市场源插件"""
    import os

    enabled = {
        s.strip().lower()
        for s in os.getenv("KQ_MARKET_SOURCES", "binance,mt5,ths").split(",")
        if s.strip()
    }
    if "binance" in enabled:
        from klinequant.gateway.market_sources.binance_source import BinanceSource
        market_manager.register(BinanceSource())
    if "mt5" in enabled:
        from klinequant.gateway.market_sources.mt5_source import Mt5Source
        mt5 = Mt5Source()
        if mt5.available:
            market_manager.register(mt5)
        else:
            logger.warning(
                "MT5 source not ready at bootstrap: 本机 MT5 终端未连接"
                "（需终端已登录运行，或配置 MT5_TERMINAL_PATH）；转后台重试，就绪后自动上线"
            )
            market_manager.defer(mt5)
    if "ths" in enabled:
        from klinequant.gateway.market_sources.ths_source import ThsSource
        ths = ThsSource()
        if ths.available:
            market_manager.register(ths)
        else:
            logger.warning(
                "THS source not ready at bootstrap: 同花顺行情服务器连接失败"
                "（检查网络或 THS_USERNAME/THS_PASSWORD）；转后台重试，就绪后自动上线"
            )
            market_manager.defer(ths)
