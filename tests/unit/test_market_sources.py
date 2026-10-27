"""市场源插件框架单元测试

覆盖：
    - MarketSourceManager：注册/查询/默认所/订阅路由（新旧主题格式）/去重广播
"""
from __future__ import annotations

from klinequant.gateway.market_sources.base import MarketSource
from klinequant.gateway.market_sources.base import price_decimals
from klinequant.gateway.market_sources.manager import MarketSourceManager
from klinequant.gateway.ws import ws_manager


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


class _DeferredFakeSource(_FakeSource):
    """启动未就绪的桩：_try_reconnect 第 ready_after 次调用后转可用"""

    def __init__(self, name: str, ready_after: int = 1):
        super().__init__(name)
        self.available = False
        self.attempts = 0
        self._ready_after = ready_after

    def _try_reconnect(self) -> None:
        self.attempts += 1
        if self.attempts >= self._ready_after:
            self.available = True


# ─── Manager：注册与路由 ───

def test_register_get_default_exchange():
    mgr = MarketSourceManager()
    assert mgr.get("binance") is None
    assert mgr.default_exchange() == ""
    mgr.register(_FakeSource("fx"))
    assert mgr.default_exchange() == "fx"          # 无 binance 时取首个注册插件
    mgr.register(_FakeSource("binance"))
    assert mgr.default_exchange() == "binance"     # binance 优先
    assert mgr.get("FX") is mgr.get("fx")          # 大小写不敏感
    assert {s.name for s in mgr.list_sources()} == {"binance", "fx"}


def test_meta_contract():
    meta = _FakeSource("fx").meta()
    assert meta["exchange"] == "fx"
    # 派生层统一下发：原生档位 + 1w + 1M/1Q/1Y（网关从日 K 聚合，前端按钮不逐源禁用）
    assert meta["timeframes"] == ["1M", "1Q", "1Y", "1h", "1m", "1w"]
    assert meta["default_symbols"][0]["symbol"] == "FXUSD"
    assert _FakeSource("otc").meta()["supports_volume"] is False   # OTC 所声明无成交量
    assert meta["region"] == "global"                          # 区域缺省国外（国内源自行覆盖为 cn）
    assert meta["tz_offset_s"] == 0                            # 交易所时区偏移缺省 0


# ─── 全量品种目录：插件默认实现 + manager TTL 缓存 ───


def test_base_list_symbols_default():
    """未覆盖 list_symbols 的插件：回退 default_symbols（type 缺失时为空串）"""
    import asyncio
    rows = asyncio.run(_FakeSource("fx").list_symbols())
    # code 缺省同 symbol（非国内源展示码 = 路由码，前端零分支）
    assert rows == [{"symbol": "FXUSD", "name": "FX/USD", "type": "", "code": "FXUSD"}]


def test_manager_list_symbols_cached():
    """TTL 内重复调用命中缓存（插件只拉取一次）；未注册源返回 None"""
    import asyncio

    class _CountingSource(_FakeSource):
        def __init__(self):
            super().__init__("cnt")
            self.calls = 0

        async def list_symbols(self):
            self.calls += 1
            return [{"symbol": "CNTUSD", "name": "CNT/USD", "type": "forex"}]

    mgr = MarketSourceManager()
    src = _CountingSource()
    mgr.register(src)
    assert asyncio.run(mgr.list_symbols("cnt"))[0]["symbol"] == "CNTUSD"
    asyncio.run(mgr.list_symbols("cnt"))
    assert src.calls == 1
    assert asyncio.run(mgr.list_symbols("nope")) is None


# ─── 价格精度：从订阅到的价格推导，随响应下发（前端只渲染不推导） ───

def test_price_decimals_from_raw_strings():
    """交易所按 tick 补齐的字符串价格：去尾零后计小数位"""
    assert price_decimals(["65239.14000000", "65240.00000000"]) == 2    # BTC tick 0.01
    assert price_decimals(["0.07068000"]) == 5                          # DOGE tick 0.00001
    assert price_decimals(["1.08432"]) == 5                             # 外汇 5 位
    assert price_decimals(["71420"]) == 0                               # 整数价
    assert price_decimals([]) == 0
    assert price_decimals(["0.123456789"], cap=8) == 8                  # 上限截断


def test_price_decimals_from_floats():
    assert price_decimals([7.142]) == 3
    assert price_decimals([65239.14]) == 2


def test_track_prec_monotonic_and_case():
    """精度缓存只增不减（新批次碰巧整数价不回退），品种名大小写不敏感"""
    src = _FakeSource("mockex")
    assert src.price_precision("BTCUSDT") == 0          # 未订阅数据时未知
    src._track_prec("btcusdt", ["65000.00000000"])      # 整数价不抬升
    assert src.price_precision("BTCUSDT") == 0
    src._track_prec("BTCUSDT", ["65239.14000000"])
    assert src.price_precision("btcusdt") == 2
    src._track_prec("BTCUSDT", ["66000.00000000"])      # 后续整数批次不回退
    assert src.price_precision("BTCUSDT") == 2


def test_klines_response_delivers_price_precision():
    """/api/market/klines 响应携带后端推导的 price_precision（前端只消费）"""
    from fastapi.testclient import TestClient
    from klinequant.gateway.app import create_app
    from klinequant.gateway.market_sources.manager import market_manager

    class _PrecSource(_FakeSource):
        name = "mockex"

        async def fetch_klines(self, symbol, timeframe, limit=200, end_time=None):
            # 模拟币安：原始字符串价格按 tick 补齐，转 float 前先推导精度
            self._track_prec(symbol, ["65239.14000000", "65240.10000000"])
            return [{"timestamp": 1, "open": 65239.14, "high": 65240.1,
                     "low": 65238.0, "close": 65239.14, "volume": 1.0, "event_ms": 0}]

    src = _PrecSource("mockex")
    market_manager.register(src)
    try:
        client = TestClient(create_app())
        resp = client.get("/api/market/klines?symbol=BTCUSDT&timeframe=1m&exchange=mockex")
        data = resp.json()
        assert resp.status_code == 200
        assert data["price_precision"] == 2
        assert data["tz_offset_s"] == 0   # 交易所时区偏移随 klines 下发（桩源缺省 0）
    finally:
        market_manager._sources.pop("mockex", None)


def test_active_targets_topic_routing():
    mgr = MarketSourceManager()
    mgr.register(_FakeSource("binance"))
    mgr.register(_FakeSource("fx"))

    # 新格式主题 klines.{exchange}.{symbol}.{tf}
    ws_manager.subscribe("ws-t1", "klines.fx.EURUSD.1m")
    # 旧格式主题 klines.{symbol}.{tf} 归默认所（binance）
    ws_manager.subscribe("ws-t2", "klines.BTCUSDT.15m")
    try:
        assert mgr._active_targets("fx") == {("EURUSD", "1m")}
        assert mgr._active_targets("binance") == {("BTCUSDT", "15m")}
        assert mgr._active_targets("other") == set()
    finally:
        ws_manager.disconnect("ws-t1")
        ws_manager.disconnect("ws-t2")


def test_active_targets_per_source_derived_routing():
    """同一 4h 订阅按源分流：原生直供源走 4h，缺供源（如 ths）映射为基础 1m 隐式订阅。
    日内自定义倍率（10m）同理映射 1m；日级派生（2d）映射 1d。防回归：
    不得因 4h 属全局 VALID_TIMEFRAMES 而错当原生流下发给不支持它的源。"""
    mgr = MarketSourceManager()
    rich = _FakeSource("rich")                       # 原生支持 4h
    rich.supported_timeframes = {"1m", "1h", "4h", "1d"}
    thin = _FakeSource("thin")                       # 仅 1m/1h/1d（无 4h，似 ths）
    thin.supported_timeframes = {"1m", "1h", "1d"}
    mgr.register(rich)
    mgr.register(thin)

    ws_manager.subscribe("ws-r", "klines.rich.BTCUSDT.4h")
    ws_manager.subscribe("ws-t", "klines.thin.600000.4h")
    ws_manager.subscribe("ws-m", "klines.thin.600000.10m")
    ws_manager.subscribe("ws-d", "klines.thin.600000.2d")
    try:
        assert mgr._active_targets("rich") == {("BTCUSDT", "4h")}          # 原生直供
        # thin 无 4h/10m → 均映射 1m；2d → 1d
        assert mgr._active_targets("thin") == {("600000", "1m"), ("600000", "1d")}
    finally:
        for cid in ("ws-r", "ws-t", "ws-m", "ws-d"):
            ws_manager.disconnect(cid)


def test_active_targets_watched_fallback():
    mgr = MarketSourceManager()
    src = _FakeSource("binance")
    src.watched_targets = [("BTCUSDT", "1h")]
    mgr.register(src)
    assert mgr.active_targets("binance") == {("BTCUSDT", "1h")}


# ─── 启动未就绪源：后台补注册（启动竞态根治） ───


def test_defer_registers_when_reconnect_succeeds():
    """defer 的源在 _try_reconnect 就绪后被 _pending_loop 补注册并移出待办"""
    import asyncio

    mgr = MarketSourceManager()
    src = _DeferredFakeSource("mt5", ready_after=1)
    mgr.defer(src)
    assert src.name not in mgr._sources          # 尚未注册
    asyncio.run(mgr._pending_loop())             # 首轮即就绪 → 注册后循环退出
    assert mgr.get("mt5") is src
    assert mgr._pending == []


def test_defer_retries_until_ready(monkeypatch):
    """首轮未就绪时按节奏重试，直至 available 转真才注册（不永久跳过）"""
    import asyncio

    import klinequant.gateway.market_sources.manager as mgr_mod
    monkeypatch.setattr(mgr_mod, "_PENDING_RETRY_INTERVAL", 0)   # 免真实等待

    mgr = MarketSourceManager()
    src = _DeferredFakeSource("mt5", ready_after=3)
    mgr.defer(src)
    asyncio.run(mgr._pending_loop())
    assert src.attempts == 3                     # 第 3 次才就绪
    assert mgr.get("mt5") is src
    assert mgr._pending == []


async def test_publish_bar_dedup_and_exchange_field(monkeypatch):
    mgr = MarketSourceManager()
    sent = []

    async def fake_publish(topic, data):
        sent.append((topic, data))
        return 1

    monkeypatch.setattr(ws_manager, "publish", fake_publish)
    bar = {"timestamp": 1000, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 0, "event_ms": 1000}

    assert await mgr.publish_bar("fx", "EURUSD", "1m", bar) is True
    # 同签名重复 bar → 去重不广播
    assert await mgr.publish_bar("fx", "EURUSD", "1m", dict(bar)) is False
    # 不同所同 bar 不去重（cache_key 含 exchange）
    assert await mgr.publish_bar("binance", "EURUSD", "1m", dict(bar)) is True

    topics = [t for t, _ in sent]
    assert "klines.EURUSD.1m" in topics and "klines.EURUSD" in topics   # 新旧主题都广播
    assert all(d["exchange"] in ("fx", "binance") for _, d in sent)     # payload 带 exchange 维度


async def test_feed_derived_intraday_from_1m(monkeypatch):
    """实时链路：1m 基础流喂入 → 合成订阅的 10m 派生 bar（日内重采样）。

    验证桶内 OHLC 合并/量求和与桶翻滚：首根开桶、末根满后下一根开新桶。
    """
    mgr = MarketSourceManager()
    src = _FakeSource("thin")
    src.supported_timeframes = {"1m", "1h", "1d"}   # 无 10m，需由 1m 派生

    async def _no_hist(*a, **k):   # seed 预填无历史（桶首根开始喂，无需预填）
        return []
    src.fetch_klines = _no_hist
    mgr.register(src)

    sent = []

    async def fake_publish(topic, data):
        sent.append((topic, data))
        return 1
    monkeypatch.setattr(ws_manager, "publish", fake_publish)

    ws_manager.subscribe("ws-d", "klines.thin.AAPL.10m")
    try:
        t0 = 1_800_000_000_000   # 10m 对齐的 epoch 毫秒
        feed = [
            (t0, 100, 110, 90, 105, 10),
            (t0 + 60_000, 105, 120, 100, 115, 20),
            (t0 + 120_000, 115, 130, 95, 125, 30),
            (t0 + 600_000, 125, 128, 120, 127, 5),   # 下一桶首根
        ]
        for ts, o, h, lo, c, v in feed:
            await mgr.publish_bar("thin", "AAPL", "1m", {
                "timestamp": ts, "open": o, "high": h, "low": lo, "close": c,
                "volume": v, "event_ms": ts,
            })
    finally:
        ws_manager.disconnect("ws-d")

    derived = [d for t, d in sent if t == "klines.thin.AAPL.10m"]
    assert derived, "未发布派生 10m bar"
    # 首桶（t0）最终聚合：开 100 / 高 130 / 低 90 / 收 125 / 量 60
    bucket0 = [b for b in derived if b["timestamp"] == t0][-1]
    assert (bucket0["open"], bucket0["high"], bucket0["low"], bucket0["close"]) == (
        100, 130, 90, 125,
    )
    assert bucket0["volume"] == 60
    # 次桶（t0+600000）由末根开启：开 125 / 量 5
    bucket1 = [b for b in derived if b["timestamp"] == t0 + 600_000][-1]
    assert bucket1["open"] == 125 and bucket1["volume"] == 5


async def test_feed_derived_skips_native_hour(monkeypatch):
    """防双口径：源原生直供的 4h 订阅不得从 1m 流重复聚合（实时 bar 由源流直接发布）"""
    mgr = MarketSourceManager()
    src = _FakeSource("rich")
    src.supported_timeframes = {"1m", "4h", "1d"}   # 4h 原生

    async def _no_hist(*a, **k):
        return []
    src.fetch_klines = _no_hist
    mgr.register(src)
    sent = []

    async def fake_publish(topic, data):
        sent.append((topic, data))
        return 1
    monkeypatch.setattr(ws_manager, "publish", fake_publish)

    ws_manager.subscribe("ws-h", "klines.rich.BTCUSDT.4h")
    try:
        t0 = 1_800_000_000_000
        await mgr.publish_bar("rich", "BTCUSDT", "1m", {
            "timestamp": t0, "open": 1, "high": 2, "low": 0.5, "close": 1.5,
            "volume": 7, "event_ms": t0,
        })
    finally:
        ws_manager.disconnect("ws-h")
    # 4h 为 rich 原生档：1m 喂入不应产生 klines.rich.BTCUSDT.4h 的派生聚合
    assert not [d for t, d in sent if t == "klines.rich.BTCUSDT.4h"]
