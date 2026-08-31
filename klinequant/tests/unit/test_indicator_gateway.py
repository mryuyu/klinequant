"""指标网关单元测试（IND-102 三端闭环后端侧）

覆盖：
    - GET /api/indicator/meta：注册指标元数据（display_meta 契约）
    - GET /api/indicator/history：注册+预热+有效序列（剔除预热段），幂等复用
    - indicator_service.on_bar：实时 bar → 增量计算 → indicators.* WS 推送
"""
from __future__ import annotations

import json

import polars as pl
import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.market_sources import kline_cache
from gateway.market_sources.base import MarketSource
from gateway.market_sources.manager import market_manager
from gateway.state import state
from gateway.ws import ws_manager


# ─── 测试辅助 ───

def _gen_bars(n: int, base_ts: int = 1700000000000) -> list[dict]:
    """确定性伪随机 bar 序列（LCG）"""
    bars, x = [], 7
    for i in range(n):
        x = (x * 1103515245 + 12345) % (2 ** 31)
        price = 100.0 + (x % 1000) / 10.0
        bars.append({
            "timestamp": base_ts + i * 60000,
            "open": price - 0.5, "high": price + 1.0, "low": price - 1.0,
            "close": price, "volume": 100.0, "event_ms": 0,
        })
    return bars


class _FakeSource(MarketSource):
    """固定吐出合成 bar 的假市场源（记录拉取次数验证预热幂等）"""

    name = "mockex"
    label = "Mock Exchange"
    supported_timeframes = {"1m"}
    supports_volume = True
    default_symbols = [{"symbol": "MOCKUSD", "name": "MOCK/USD"}]
    watched_targets = []

    def __init__(self, bars: list[dict]):
        self._bars = bars
        self.fetch_calls = 0

    async def fetch_klines(self, symbol, timeframe, limit=200, end_time=None):
        self.fetch_calls += 1
        bars = self._bars
        if end_time is not None:
            bars = [b for b in bars if b["timestamp"] <= end_time]
        return bars[-limit:]

    async def stream_loop(self):
        pass


@pytest.fixture
def client():
    app = create_app()
    return TestClient(app)


@pytest.fixture(autouse=True)
def _isolate():
    """每个用例独立引擎实例 + 清理假市场源注册与 K 线进程缓存"""
    state._indicator_engine = None
    kline_cache.clear()
    yield
    market_manager._sources.pop("mockex", None)
    state._indicator_engine = None
    kline_cache.clear()


MACD_PARAMS = {"fast_period": 12, "slow_period": 26, "signal_period": 9}


def _macd_full_last(closes: list[float]) -> dict:
    from core.indicator_engine.indicators import MACD
    df = pl.DataFrame({
        "timestamp": [1700000000000 + i * 60000 for i in range(len(closes))],
        "open": closes, "high": closes, "low": closes, "close": closes,
        "volume": [1.0] * len(closes),
    })
    result = MACD(params=MACD_PARAMS).calculate(df)
    return {
        "DIF": result["MACD_12_26_9_DIF"][-1],
        "DEA": result["MACD_12_26_9_DEA"][-1],
        "HIST": result["MACD_12_26_9_HIST"][-1],
    }


# ─── /api/indicator/meta ───

class TestIndicatorMeta:
    def test_meta_lists_macd_display_meta(self, client):
        """MACD 元数据含字段/值域/默认参数（display_meta 契约）"""
        resp = client.get("/api/indicator/meta")
        assert resp.status_code == 200
        items = {i["name"]: i for i in resp.json()["indicators"]}
        assert "MACD" in items
        macd = items["MACD"]
        assert macd["display_meta"]["fields"] == ["DIF", "DEA", "HIST"]
        assert macd["display_meta"]["range"] == "zero_symmetric"
        assert macd["min_periods"] == 34
        assert macd["default_params"] == MACD_PARAMS


# ─── /api/indicator/history ───

class TestIndicatorHistory:
    def test_history_warmed_valid_series(self, client):
        """预热后返回剔除预热段的有效序列，末值与全量计算一致"""
        bars = _gen_bars(120)
        market_manager.register(_FakeSource(bars))

        resp = client.get(
            "/api/indicator/history?symbol=MOCKUSD&timeframe=1m&exchange=mockex"
            f"&indicator=MACD&limit=60&params={json.dumps(MACD_PARAMS)}"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["warmed"] is True
        assert data["count"] == 60

        # 拉取深度 = 显示需求 60 + 预热 34 = 94；有效序列 = 94 - 34 + 1 = 61 条
        # （第 34 根即输出首个值），limit=60 截取末 60 条 → 起点为第 35 根
        fetched = bars[-94:]
        assert data["data"][0]["timestamp"] == fetched[34]["timestamp"]
        expect = _macd_full_last([b["close"] for b in fetched])
        got = data["data"][-1]["values"]
        assert got["DIF"] == pytest.approx(expect["DIF"], rel=1e-9)
        assert got["DEA"] == pytest.approx(expect["DEA"], rel=1e-9)
        assert got["HIST"] == pytest.approx(expect["HIST"], rel=1e-9)

    def test_history_idempotent_no_refetch(self, client):
        """同契约 key 同深度二次请求复用已预热实例，不重复拉取历史"""
        src = _FakeSource(_gen_bars(120))
        market_manager.register(src)

        url = (
            "/api/indicator/history?symbol=MOCKUSD&timeframe=1m&exchange=mockex"
            f"&indicator=MACD&limit=60&params={json.dumps(MACD_PARAMS)}"
        )
        assert client.get(url).json()["warmed"] is True
        assert client.get(url).json()["warmed"] is True
        assert src.fetch_calls == 1

    def test_history_deepens_on_larger_limit(self, client):
        """显示需求增大（懒加载翻页）：向后分页加深预热深度，末值与全量对拍"""
        bars = _gen_bars(1500)
        src = _FakeSource(bars)
        market_manager.register(src)

        base = (
            "/api/indicator/history?symbol=MOCKUSD&timeframe=1m&exchange=mockex"
            "&indicator=MACD&params="
        )
        assert client.get(base + json.dumps(MACD_PARAMS) + "&limit=60").json()["count"] == 60
        assert src.fetch_calls == 1  # 首预热单页 94 根

        resp = client.get(base + json.dumps(MACD_PARAMS) + "&limit=1200")
        data = resp.json()
        assert data["warmed"] is True
        assert data["count"] == 1200
        # 目标深度 1200+34=1234：缓存层尾刷 3 根 + 缺口单页补齐（1140 根），共 3 次拉取
        assert src.fetch_calls == 3
        fetched = bars[-1234:]
        expect = _macd_full_last([b["close"] for b in fetched])
        got = data["data"][-1]["values"]
        assert got["DIF"] == pytest.approx(expect["DIF"], rel=1e-9)
        assert got["DEA"] == pytest.approx(expect["DEA"], rel=1e-9)
        assert got["HIST"] == pytest.approx(expect["HIST"], rel=1e-9)

    def test_history_unknown_indicator(self, client):
        """未注册指标：降级返回空序列（不 500）"""
        market_manager.register(_FakeSource(_gen_bars(50)))
        resp = client.get(
            "/api/indicator/history?symbol=MOCKUSD&timeframe=1m&exchange=mockex"
            "&indicator=NOPE&limit=30"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["warmed"] is False and data["count"] == 0

    def test_history_unknown_exchange(self, client):
        """未知市场源：降级返回空序列"""
        resp = client.get(
            "/api/indicator/history?symbol=BTCUSDT&timeframe=1m&exchange=nosuch"
            "&indicator=MACD&limit=30"
        )
        assert resp.status_code == 200
        assert resp.json()["count"] == 0


# ─── indicator_service.on_bar：增量计算 → WS 推送 ───

def _warmup_engine(params: dict, bars: list[dict], symbol="BTCUSDT"):
    from gateway.indicator_service import _bars_to_df
    engine = state.indicator_engine
    engine.ensure_indicator("MACD", params, symbol, "binance", "1m")
    engine.warmup(symbol, "binance", "1m", _bars_to_df(bars))
    return engine


async def test_on_bar_publishes_indicator_update(monkeypatch):
    """实时 bar 驱动增量计算并推送 indicators.* 主题"""
    from gateway.indicator_service import on_bar

    bars = _gen_bars(100)
    engine = _warmup_engine({}, bars)

    sent = []

    async def fake_publish(topic, data):
        sent.append((topic, data))
        return 1

    monkeypatch.setattr(ws_manager, "publish", fake_publish)

    bar = dict(bars[-1])
    bar["timestamp"] = bars[-1]["timestamp"] + 60000
    bar["close"] = bars[-1]["close"] + 1.0
    await on_bar("binance", "BTCUSDT", "1m", bar)

    assert len(sent) == 1
    topic, payload = sent[0]
    assert topic == "indicators.binance.BTCUSDT.1m"
    assert payload[0]["indicator"] == "MACD"
    assert set(payload[0]["values"].keys()) == {"DIF", "DEA", "HIST"}

    # 值与全量计算对拍
    expect = _macd_full_last([b["close"] for b in bars] + [bar["close"]])
    assert payload[0]["values"]["DIF"] == pytest.approx(expect["DIF"], rel=1e-9)


async def test_on_bar_skips_without_indicators(monkeypatch):
    """无注册指标的品种：零开销跳过，不推送"""
    from gateway.indicator_service import on_bar

    sent = []

    async def fake_publish(topic, data):
        sent.append(topic)
        return 1

    monkeypatch.setattr(ws_manager, "publish", fake_publish)
    await on_bar("binance", "OTHERUSDT", "1m", _gen_bars(1)[-1])
    assert sent == []
