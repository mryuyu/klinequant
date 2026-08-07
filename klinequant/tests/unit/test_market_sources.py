"""市场源插件框架单元测试

覆盖：
    - MarketSourceManager：注册/查询/默认所/订阅路由（新旧主题格式）/去重广播
    - IgClient：IG 时间解析、resolution 映射、candle→bar、/prices 分页拼接
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gateway.market_sources.base import MarketSource
from gateway.market_sources.ig_client import (
    RESOLUTION_MAP,
    IgClient,
    normalize_bar,
    normalize_rate,
    parse_ig_time,
)
from gateway.market_sources.manager import MarketSourceManager
from gateway.ws import ws_manager


class _FakeSource(MarketSource):
    """最小可用插件桩"""

    def __init__(self, name: str):
        self.name = name
        self.label = f"Fake {name}"
        self.supported_timeframes = {"1m", "1h"}
        self.supports_volume = name != "otc"
        self.default_symbols = [{"symbol": f"{name.upper()}USD", "name": f"{name.upper()}/USD"}]
        self.watched_targets = []

    async def fetch_klines(self, symbol, timeframe, limit=200, end_time=None):
        return [{"timestamp": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 0, "event_ms": 0}]

    async def stream_loop(self):
        pass


# ─── Manager：注册与路由 ───

def test_register_get_default_exchange():
    mgr = MarketSourceManager()
    assert mgr.get("binance") is None
    assert mgr.default_exchange() == ""
    mgr.register(_FakeSource("ig"))
    assert mgr.default_exchange() == "ig"          # 无 binance 时取首个注册插件
    mgr.register(_FakeSource("binance"))
    assert mgr.default_exchange() == "binance"     # binance 优先
    assert mgr.get("IG") is mgr.get("ig")          # 大小写不敏感
    assert {s.name for s in mgr.list_sources()} == {"binance", "ig"}


def test_meta_contract():
    meta = _FakeSource("ig").meta()
    assert meta["exchange"] == "ig"
    assert meta["timeframes"] == ["1h", "1m"]
    assert meta["default_symbols"][0]["symbol"] == "IGUSD"
    assert _FakeSource("otc").meta()["supports_volume"] is False   # OTC 所声明无成交量


# 分页测试用：以固定基准生成合法 IG 时间格式的 candle
_BASE = datetime(2026, 8, 7, 0, 0, tzinfo=timezone.utc)


def _candle_at(minutes: int) -> dict:
    dt = _BASE + timedelta(minutes=minutes)
    return {
        "snapshotTime": dt.strftime("%Y:%m:%d-%H:%M:%S"),
        "lastBid": {"openBid": "1", "highBid": "1", "lowBid": "1", "closeBid": "1"},
    }


def test_active_targets_topic_routing():
    mgr = MarketSourceManager()
    mgr.register(_FakeSource("binance"))
    mgr.register(_FakeSource("ig"))

    # 新格式主题 klines.{exchange}.{symbol}.{tf}
    ws_manager.subscribe("ws-t1", "klines.ig.EURUSD.1m")
    # 旧格式主题 klines.{symbol}.{tf} 归默认所（binance）
    ws_manager.subscribe("ws-t2", "klines.BTCUSDT.15m")
    try:
        assert mgr._active_targets("ig") == {("EURUSD", "1m")}
        assert mgr._active_targets("binance") == {("BTCUSDT", "15m")}
        assert mgr._active_targets("other") == set()
    finally:
        ws_manager.disconnect("ws-t1")
        ws_manager.disconnect("ws-t2")


def test_active_targets_watched_fallback():
    mgr = MarketSourceManager()
    src = _FakeSource("binance")
    src.watched_targets = [("BTCUSDT", "1h")]
    mgr.register(src)
    assert mgr.active_targets("binance") == {("BTCUSDT", "1h")}


async def test_publish_bar_dedup_and_exchange_field(monkeypatch):
    mgr = MarketSourceManager()
    sent = []

    async def fake_publish(topic, data):
        sent.append((topic, data))
        return 1

    monkeypatch.setattr(ws_manager, "publish", fake_publish)
    bar = {"timestamp": 1000, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 0, "event_ms": 1000}

    assert await mgr.publish_bar("ig", "EURUSD", "1m", bar) is True
    # 同签名重复 bar → 去重不广播
    assert await mgr.publish_bar("ig", "EURUSD", "1m", dict(bar)) is False
    # 不同所同 bar 不去重（cache_key 含 exchange）
    assert await mgr.publish_bar("binance", "EURUSD", "1m", dict(bar)) is True

    topics = [t for t, _ in sent]
    assert "klines.EURUSD.1m" in topics and "klines.EURUSD" in topics   # 新旧主题都广播
    assert all(d["exchange"] in ("ig", "binance") for _, d in sent)     # payload 带 exchange 维度


# ─── IG 时间解析与映射 ───

def test_parse_ig_time():
    assert parse_ig_time("2026:08:07-10:30:00") == 1786098600000
    assert parse_ig_time("2026:08:07-10:30:00.123") == 1786098600123
    assert parse_ig_time("  2026:08:07-10:30:00  ") == 1786098600000
    # v3 snapshotTimeUTC（ISO/UTC）与 v1 同一时刻等价
    assert parse_ig_time("2026-08-07T10:30:00") == 1786098600000


def test_resolution_map_covers_frontend_timeframes():
    # 前端 TF_VALID 的全部周期 IG 均应有映射（3d/1w 除外，IG 不支持）
    for tf in ["1m", "5m", "15m", "1h", "4h", "1d"]:
        assert tf in RESOLUTION_MAP
    assert "3d" not in RESOLUTION_MAP and "1w" not in RESOLUTION_MAP


def test_candle_to_bar_uses_bid_and_zero_volume():
    # v3 结构（demo 实测）：openPrice/highPrice/lowPrice/closePrice 的 bid 字段
    p = {
        "snapshotTime": "2026/08/07 18:30:00",
        "snapshotTimeUTC": "2026-08-07T10:30:00",
        "openPrice": {"bid": 1.0801, "ask": 1.0802},
        "highPrice": {"bid": 1.0815, "ask": 1.0816},
        "lowPrice": {"bid": 1.0799, "ask": 1.0800},
        "closePrice": {"bid": 1.0810, "ask": 1.0811},
        "lastTradedVolume": 44,
    }
    bar = IgClient.candle_to_bar(p, event_ms=42)
    assert bar["timestamp"] == 1786098600000       # 取 snapshotTimeUTC（UTC）
    assert bar["open"] == pytest.approx(1.0801)
    assert bar["close"] == pytest.approx(1.0810)
    assert bar["volume"] == 0.0        # OTC 无成交量
    assert bar["event_ms"] == 42

    # v1 结构兼容（lastBid.openBid 等）
    p1 = {
        "snapshotTime": "2026:08:07-10:30:00",
        "lastBid": {"openBid": "1.0801", "highBid": "1.0815", "lowBid": "1.0799", "closeBid": "1.0810"},
    }
    assert IgClient.candle_to_bar(p1)["close"] == pytest.approx(1.0810)


# ─── IG /prices 分页拼接 ───

async def test_fetch_prices_pagination(monkeypatch):
    client = IgClient(api_key="k", identifier="u", password="p")

    calls = []

    async def fake_request(method, path, params=None, _retry=True, version="2"):
        calls.append((path, dict(params or {}), version))
        # 第一页（无 endDate）：分钟 0~999；第二页：更早的 -500~-1
        if "endDate" not in (params or {}):
            return {"prices": [_candle_at(i) for i in range(1000)]}
        return {"prices": [_candle_at(i) for i in range(-500, 0)]}

    monkeypatch.setattr(client, "_request", fake_request)
    prices = await client.fetch_prices("EPIC", "MINUTE", 2000)

    assert len(calls) == 2
    assert all(v == "3" for _, _, v in calls)      # /prices 必须走 Version 3
    assert "endDate" not in calls[0][1] and calls[0][1]["max"] == 1000
    assert "endDate" in calls[1][1] and calls[1][1]["max"] == 1000
    assert calls[1][1]["endDate"] == "2026-08-06T23:59:59"   # 首根开始时间前 1 秒
    assert len(prices) == 1500                     # 两页合并
    ts = [parse_ig_time(p["snapshotTime"]) for p in prices]
    assert ts == sorted(ts)                        # 升序
    assert len(set(ts)) == len(ts)                 # 无重复


async def test_fetch_prices_respects_limit(monkeypatch):
    client = IgClient(api_key="k", identifier="u", password="p")

    async def fake_request(method, path, params=None, _retry=True, version="2"):
        return {"prices": [_candle_at(i) for i in range(params["max"])]}

    monkeypatch.setattr(client, "_request", fake_request)
    prices = await client.fetch_prices("EPIC", "MINUTE", 100)
    assert len(prices) == 100


# ─── IG epic 解析 ───

async def test_resolve_epic_builtin_and_search(monkeypatch):
    client = IgClient(api_key="k", identifier="u", password="p")
    # 内置映射直接命中（demo 环境 spot CFD 命名）
    assert await client.resolve_epic("eurusd") == "CS.D.EURUSD.CFD.IP"

    async def fake_request(method, path, params=None, _retry=True, version="2"):
        assert path == "/markets" and params["searchTerm"] == "GBPPLN"
        # demo /markets 平铺结构：期权排在前面，应跳过期权选 CURRENCIES 现货
        return {"markets": [
            {"epic": "DO.D.GBPPLN.1.IP", "instrumentType": "OPT_CURRENCIES"},
            {"epic": "CS.D.GBPPLN.CFD.IP", "instrumentType": "CURRENCIES"},
        ]}

    monkeypatch.setattr(client, "_request", fake_request)
    assert await client.resolve_epic("GBPPLN") == "CS.D.GBPPLN.CFD.IP"
    # 搜索结果已缓存：再次解析不再请求
    assert await client.resolve_epic("GBPPLN") == "CS.D.GBPPLN.CFD.IP"


# ─── 点位报价归一化（demo EURUSD 以汇率×10000 报价） ───

def test_normalize_rate_points_to_rate():
    # 汇率型货币对：价格超合理区间（>10）视为点位，除 10000 还原
    assert normalize_rate("EURUSD", 11561.5) == pytest.approx(1.15615)
    # 合理区间内的价格原样返回（正常汇率报价）
    assert normalize_rate("GBPUSD", 1.3496) == pytest.approx(1.3496)
    # 非汇率型货币对不做归一（USDJPY 157 / XAUUSD 4395 都是合法报价）
    assert normalize_rate("USDJPY", 157.43) == pytest.approx(157.43)
    assert normalize_rate("XAUUSD", 4395.5) == pytest.approx(4395.5)


def test_normalize_bar_ohlc_only():
    bar = {"timestamp": 1, "open": 11561.0, "high": 11563.0, "low": 11560.0,
           "close": 11562.0, "volume": 0.0, "event_ms": 0}
    n = normalize_bar("EURUSD", bar)
    assert n["open"] == pytest.approx(1.1561)
    assert n["high"] == pytest.approx(1.1563)
    assert n["low"] == pytest.approx(1.1560)
    assert n["close"] == pytest.approx(1.1562)
    # 时间戳/成交量不受影响
    assert n["timestamp"] == 1 and n["volume"] == 0.0


# ─── IG 历史 K 线累积缓存（demo /prices 每次仅返回约 20 根） ───

class _FakeIgClient:
    """fetch_klines 所需最小接口：按批次吐回 candle"""

    available = True

    def __init__(self, batches):
        self._batches = list(batches)

    async def resolve_epic(self, symbol):
        return "CS.D.EURUSD.CFD.IP"

    async def fetch_prices(self, epic, resolution, limit, end_time=None):
        return self._batches.pop(0)


def _v3_candle(ts_utc: str, price: float) -> dict:
    return {
        "snapshotTimeUTC": ts_utc,
        "openPrice": {"bid": price}, "highPrice": {"bid": price},
        "lowPrice": {"bid": price}, "closePrice": {"bid": price},
    }


async def test_fetch_klines_accumulates_cache():
    from gateway.market_sources.ig_source import IgSource

    b1 = [_v3_candle(f"2026-08-07T10:{i:02d}:00", 1.15 + i * 0.0001) for i in range(3)]
    # 第二批与第一批重叠 1 根，并延续 2 根新 bar
    b2 = [_v3_candle(f"2026-08-07T10:{i:02d}:00", 1.15 + i * 0.0001) for i in range(2, 5)]
    src = IgSource(client=_FakeIgClient([b1, b2]))

    first = await src.fetch_klines("EURUSD", "1m", 100)
    assert len(first) == 3
    second = await src.fetch_klines("EURUSD", "1m", 100)
    assert len(second) == 5                      # 两次拉取累积去重，突破单次返回上限
    ts = [b["timestamp"] for b in second]
    assert ts == sorted(ts)                      # 升序

    # end_time 向前翻页：只返回累积缓存中早于锚点的 bar
    anchor = second[3]["timestamp"]
    older = await src.fetch_klines("EURUSD", "1m", 100, end_time=anchor)
    assert len(older) == 3 and all(b["timestamp"] < anchor for b in older)
