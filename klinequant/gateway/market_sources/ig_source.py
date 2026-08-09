"""IG 外汇市场源插件

实时链路：Lightstreamer CANDLE 流（IG 专有协议，需 lightstreamer-client-lib）
降级链路：REST /prices 轮询最新 candle（库缺失/连接失败时自动接管）

外汇为 OTC 市场：无成交量（volume 恒 0，supports_volume=False）。
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from decimal import Decimal

from gateway.market_sources.base import MarketSource
from gateway.market_sources.ig_client import RESOLUTION_MAP, IgClient, normalize_bar, normalize_rate
from gateway.market_sources.manager import market_manager

logger = logging.getLogger(__name__)

POLL_INTERVAL = float(os.getenv("IG_POLL_INTERVAL", "10"))     # REST 降级轮询间隔（秒）
TICKER_CACHE_TTL = 30.0   # ticker 结果短缓存（秒）：前端多品种轮询时合并重复请求，缓解 IG 限流
LS_SILENT_TIMEOUT = 60.0   # Lightstreamer 静默判定（秒）
LS_RETRY_COOLDOWN = 300.0  # LS 链路故障后的冷却重试间隔（秒）
HIST_CACHE_MAX = int(os.getenv("IG_HIST_CACHE_MAX", "2000"))   # 每 (symbol, tf) 累积历史 bar 上限

# Lightstreamer 客户端库可选：缺失/初始化失败时自动降级 REST 轮询
try:
    from lightstreamer.client import (  # type: ignore
        LightstreamerClient,
        Proxy,
        Subscription,
        SubscriptionListener,
    )
    _HAS_LIGHTSTREAMER = True
except ImportError:  # pragma: no cover
    _HAS_LIGHTSTREAMER = False

# CANDLE 流字段：UTM=蜡烛起始时间(ms)，BID=当前买价，CONFIRMED=是否已收盘
_CANDLE_FIELDS = ["UTM", "BID", "CONFIRMED"]


class IgSource(MarketSource):
    """IG 外汇：Lightstreamer 主链路 + REST 轮询降级"""

    name = "ig"
    label = "IG Forex"
    supported_timeframes = set(RESOLUTION_MAP.keys())
    supports_volume = False
    default_symbols = [
        {"symbol": "EURUSD", "name": "EUR/USD"},
        {"symbol": "GBPUSD", "name": "GBP/USD"},
        {"symbol": "USDJPY", "name": "USD/JPY"},
        {"symbol": "AUDUSD", "name": "AUD/USD"},
        {"symbol": "USDCHF", "name": "USD/CHF"},
        {"symbol": "XAUUSD", "name": "XAU/USD"},
    ]
    watched_targets: list[tuple[str, str]] = []

    def __init__(self, client: IgClient | None = None):
        self._client = client or IgClient()
        self.available = self._client.available
        # Lightstreamer 运行期故障后冷却降级（非永久），避免 JVM 异常频繁重试
        self._ls_disabled_until = 0.0
        # (epic, resolution) -> 进行中的蜡烛 {ts, open, high, low, close}
        self._pending: dict[tuple[str, str], dict] = {}
        # symbol -> (ts, ticker)：短 TTL 缓存，同品种重复请求合并
        self._ticker_cache: dict[str, tuple[float, dict | None]] = {}
        # (symbol, tf) -> {ts: bar}：历史 K 线累积缓存（demo /prices 每次仅返回约 20 根，
        # 靠多次拉取 + 实时流增量逐步累积，运行越久历史越完整）
        self._hist_cache: dict[tuple[str, str], dict[int, dict]] = {}

    # ─── REST 历史 K 线 / 行情摘要 ───

    def _hist_merge(self, symbol: str, timeframe: str, bar: dict) -> None:
        """把一根 bar 并入累积缓存（同 ts 覆盖），超上限时淘汰最早的"""
        c = self._hist_cache.setdefault((symbol, timeframe), {})
        c[bar["timestamp"]] = bar
        if len(c) > HIST_CACHE_MAX:
            for ts in sorted(c)[: len(c) - HIST_CACHE_MAX]:
                del c[ts]

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
        end_time: int | None = None,
    ) -> list[dict]:
        resolution = RESOLUTION_MAP.get(timeframe)
        if not resolution:
            raise ValueError(f"IG unsupported timeframe: {timeframe}")
        sym = symbol.upper()
        epic = await self._client.resolve_epic(symbol)
        try:
            prices = await self._client.fetch_prices(epic, resolution, limit, end_time)
            for p in prices:
                bar = normalize_bar(sym, IgClient.candle_to_bar(p))
                # 从归一化后的价格推导品种显示精度（点位对已÷回汇率，随响应下发，前端只渲染）
                self._track_prec(sym, [bar["open"], bar["high"], bar["low"], bar["close"]])
                self._hist_merge(sym, timeframe, bar)
        except Exception as e:
            # API 失败时用累积缓存兜底（有则返回，无则继续上抛）
            if not self._hist_cache.get((sym, timeframe)):
                raise
            logger.warning(f"IG klines fallback to cache {sym}/{timeframe}: {e}")
        merged = sorted(
            self._hist_cache.get((sym, timeframe), {}).values(), key=lambda b: b["timestamp"]
        )
        if end_time:
            merged = [b for b in merged if b["timestamp"] < end_time]
        return merged[-limit:]

    async def fetch_ticker(self, symbol: str) -> dict | None:
        cached = self._ticker_cache.get(symbol)
        if cached and time.monotonic() - cached[0] < TICKER_CACHE_TTL:
            return cached[1]
        sym = symbol.upper()
        epic = await self._client.resolve_epic(symbol)
        data = await self._client._request("GET", f"/markets/{epic}")
        snap = data.get("snapshot") or {}
        bid = snap.get("bid")
        offer = snap.get("offer")
        if not bid and not offer:
            # snapshot 无报价（demo EURUSD 数据缺口/休市）：用最新 K 线 + 近 24 根小时 K 线构造 ticker
            result = await self._ticker_from_candles(sym, epic)
            self._ticker_cache[symbol] = (time.monotonic(), result)
            return result
        bid_n = normalize_rate(sym, float(bid))
        offer_n = normalize_rate(sym, float(offer))
        self._track_prec(
            sym,
            [bid_n, offer_n, normalize_rate(sym, float(snap.get("high") or 0)), normalize_rate(sym, float(snap.get("low") or 0))],
        )
        # 中点用 Decimal 均值避免浮点噪声污染展示价（(1.15578+1.15581)/2 可能带长尾）
        last = offer_n if bid is None else (
            bid_n if offer is None else float((Decimal(str(bid_n)) + Decimal(str(offer_n))) / 2)
        )
        result = {
            "symbol": sym,
            "last_price": float(last),
            "bid": bid_n,
            "ask": offer_n,
            "volume_24h": 0.0,
            "price_change_pct": float(snap.get("percentageChange") or 0),
            "high_24h": normalize_rate(sym, float(snap.get("high") or 0)),
            "low_24h": normalize_rate(sym, float(snap.get("low") or 0)),
        }
        self._ticker_cache[symbol] = (time.monotonic(), result)
        return result

    async def _ticker_from_candles(self, sym: str, epic: str) -> dict | None:
        """snapshot 无报价时的 ticker 构造：最新分钟 K 线收盘价 + 24 小时统计"""
        p = await self._client.fetch_latest_candle(epic, "MINUTE")
        if not p:
            return None
        close = normalize_rate(sym, IgClient.candle_to_bar(p)["close"])
        if close <= 0:
            return None
        high, low, pct = close, close, 0.0
        try:
            day = await self._client.fetch_prices(epic, "HOUR", 25)
            if day:
                bars = [normalize_bar(sym, IgClient.candle_to_bar(x)) for x in day]
                highs = [b["high"] for b in bars if b["high"] > 0]
                lows = [b["low"] for b in bars if b["low"] > 0]
                if highs:
                    high = max(highs)
                if lows:
                    low = min(lows)
                first_open = bars[0]["open"]
                if first_open > 0:
                    pct = (close / first_open - 1) * 100
        except Exception as e:
            logger.debug(f"IG ticker 24h stats fallback failed {sym}: {e}")
        # K 线构造的 ticker 同样累积精度（snapshot 无报价时此路径是前端唯一下发源）
        self._track_prec(sym, [close, high, low])
        return {
            "symbol": sym,
            "last_price": close,
            "bid": close,
            "ask": 0.0,
            "volume_24h": 0.0,
            "price_change_pct": pct,
            "high_24h": high,
            "low_24h": low,
        }

    # ─── 实时流主循环 ───

    async def stream_loop(self) -> None:
        if not self.available:
            logger.warning("IG source unavailable: credentials missing, stream loop idle")
            while True:
                await asyncio.sleep(60)
        logger.info(
            f"IG source stream started (mode={'lightstreamer' if self._ls_usable() else 'rest-poll'})"
        )
        while True:
            targets = sorted(market_manager.active_targets(self.name))
            if not targets:
                await asyncio.sleep(2)
                continue
            if self._ls_usable():
                try:
                    await self._run_lightstreamer(targets)
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"IG Lightstreamer failed: {e}, downgrade to REST polling")
                    self._ls_disabled_until = time.monotonic() + LS_RETRY_COOLDOWN
            await self._run_poll(targets)

    def _ls_usable(self) -> bool:
        return _HAS_LIGHTSTREAMER and time.monotonic() >= self._ls_disabled_until

    # ─── REST 轮询降级 ───

    async def _run_poll(self, targets: list[tuple[str, str]]) -> None:
        """轮询各目标最新 candle 并广播；订阅集变化时返回（manager 重启本循环）"""
        epic_map: dict[tuple[str, str], tuple[str, str]] = {}
        for symbol, tf in targets:
            try:
                epic_map[(symbol, tf)] = (
                    await self._client.resolve_epic(symbol), RESOLUTION_MAP[tf]
                )
            except Exception as e:
                logger.warning(f"IG poll skip {symbol}/{tf}: {e}")
        while True:
            if sorted(market_manager.active_targets(self.name)) != targets:
                return
            for (symbol, tf), (epic, resolution) in epic_map.items():
                try:
                    p = await self._client.fetch_latest_candle(epic, resolution)
                    if p:
                        bar = normalize_bar(
                            symbol, IgClient.candle_to_bar(p, event_ms=int(time.time() * 1000))
                        )
                        self._hist_merge(symbol, tf, bar)
                        await market_manager.publish_bar(self.name, symbol, tf, bar)
                except Exception as e:
                    logger.debug(f"IG poll error {symbol}/{tf}: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    # ─── Lightstreamer 主链路 ───

    async def _run_lightstreamer(self, targets: list[tuple[str, str]]) -> None:
        """连接 Lightstreamer 并订阅 CANDLE 流；静默超时/连接断开时返回重连"""
        await self._client.login()
        endpoint = self._client.lightstreamer_endpoint
        if not endpoint:
            raise RuntimeError("no lightstreamerEndpoint in IG session")

        # 目标 → (epic, resolution)，订阅项 CANDLE:{epic}:{resolution}
        items: list[str] = []
        item_map: dict[str, tuple[str, str]] = {}
        for symbol, tf in targets:
            epic = await self._client.resolve_epic(symbol)
            resolution = RESOLUTION_MAP[tf]
            item = f"CANDLE:{epic}:{resolution}"
            items.append(item)
            item_map[item] = (symbol, tf)

        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        state = {"last_event_at": time.monotonic()}

        class _Listener(SubscriptionListener):  # type: ignore[misc]
            """JVM 线程回调 → 线程安全投递到 asyncio 队列"""

            def onItemUpdate(self, update) -> None:  # noqa: N802
                try:
                    state["last_event_at"] = time.monotonic()
                    loop.call_soon_threadsafe(
                        queue.put_nowait,
                        (update.getItemName(), {f: update.getValue(f) for f in _CANDLE_FIELDS}),
                    )
                except Exception:
                    pass

        client = LightstreamerClient(endpoint, "DEFAULT")
        # 开发环境需经代理访问 IG 推送服务器（库内置 HTTP 代理支持）
        proxy_url = os.getenv("HTTP_PROXY", "")
        if proxy_url.startswith("http://"):
            try:
                hostport = proxy_url[len("http://"):].split("/")[0]
                host, _, port = hostport.partition(":")
                client.connectionOptions.setProxy(Proxy("HTTP", host, int(port or 80)))
            except Exception:
                pass
        # IG 约定：user=账户 ID，password=CST+X-SECURITY-TOKEN 拼接
        try:
            account_id = await self._account_id()
            client.connectionDetails.setUser(account_id)
            client.connectionDetails.setPassword(f"{self._client.cst}{self._client._token}")
        except Exception as e:
            client.disconnect()
            raise RuntimeError(f"IG streaming auth failed: {e}")

        sub = Subscription("MERGE", items, _CANDLE_FIELDS)
        sub.addListener(_Listener())
        client.subscribe(sub)
        client.connect()
        logger.info(f"IG Lightstreamer connecting: {endpoint} items={len(items)}")

        try:
            while True:
                # 订阅集变化 → 返回交由 manager 重启
                if sorted(market_manager.active_targets(self.name)) != targets:
                    return
                now = time.monotonic()
                if now - state["last_event_at"] > LS_SILENT_TIMEOUT:
                    logger.warning("IG Lightstreamer silent timeout, reconnecting...")
                    return
                try:
                    item, fields = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                symbol, tf = item_map.get(item, (None, None))
                if not symbol:
                    continue
                await self._on_candle_update(symbol, tf, fields)
        finally:
            try:
                client.disconnect()
            except Exception:
                pass

    async def _account_id(self) -> str:
        """会话账户 ID（Lightstreamer user 字段）：取自登录响应 currentAccount，
        IG 无顶级 GET /accounts 端点，不可另行请求"""
        await self._client.login()
        if not self._client.account_id:
            raise RuntimeError("IG login response missing currentAccount.accountId")
        return self._client.account_id

    async def _on_candle_update(self, symbol: str, tf: str, fields: dict) -> None:
        """CANDLE 流增量 → 维护进行中蜡烛的 OHLC 并广播

        IG CANDLE 流只给当前价（BID）与蜡烛起始时间（UTM），
        OHLC 由本地按 UTM 分桶累积（新 UTM 开新蜡烛）。
        """
        try:
            utm = int(fields.get("UTM") or 0)
            bid = normalize_rate(symbol, float(fields.get("BID") or 0))
        except (TypeError, ValueError):
            return
        if not utm or bid <= 0:
            return
        key = (symbol, tf)
        pending = self._pending.get(key)
        if pending is None or pending["timestamp"] != utm:
            pending = {"timestamp": utm, "open": bid, "high": bid, "low": bid, "close": bid}
            self._pending[key] = pending
        else:
            pending["high"] = max(pending["high"], bid)
            pending["low"] = min(pending["low"], bid)
            pending["close"] = bid
        bar = {**pending, "volume": 0.0, "event_ms": int(time.time() * 1000)}
        self._hist_merge(symbol, tf, bar)
        await market_manager.publish_bar(self.name, symbol, tf, bar)
        if str(fields.get("CONFIRMED") or "").lower() == "true":
            self._pending.pop(key, None)  # 蜡烛已收盘，下一根从新 UTM 开始
