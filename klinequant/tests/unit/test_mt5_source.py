"""本地 MT5 市场源插件单测（fake 驱动注入，不依赖真实终端）

driver 层返回值契约：symbol_info/symbol_info_tick/symbols_get 回传普通 dict（子进程管道回传经 _pipe_safe 转换，匿名 namedtuple 不可 pickle）。
"""
import asyncio

import pytest

from gateway.market_sources.mt5_source import TIMEFRAME_MAP, Mt5Source


class _FakeMt5:
    """驱动 fake：接口与 Mt5Api 一致，返回标准化 row dict（time=epoch 秒）"""

    def __init__(self, symbols=("EURUSD", "GBPUSD"), digits=5, rows=None, tick=None, ok=True, catalog=None):
        self.ok = ok
        self.symbols = set(symbols)
        self.digits = digits
        self.rows = rows or []
        self.tick = tick
        self.catalog = catalog   # symbols_get 返回值（终端全量品种元数据）
        self.selected: list[str] = []
        self.range_calls: list[tuple] = []
        self.shutdown_calls = 0

    def initialize(self, **kwargs):
        return self.ok

    def shutdown(self):
        self.shutdown_calls += 1

    def symbol_select(self, symbol, enable=True):
        self.selected.append(symbol)
        return True

    def symbol_info(self, symbol):
        if symbol not in self.symbols:
            return None
        return {"digits": self.digits}

    def symbol_info_tick(self, symbol):
        return self.tick

    def symbols_get(self):
        return self.catalog

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count):
        return self.rows[-count:]

    def copy_rates_range(self, symbol, timeframe, date_from, date_to):
        self.range_calls.append((symbol, timeframe, date_from, date_to))
        return self.rows


def _row(sec: int, close: float, tick_vol: int = 10, real_vol: int = 0) -> dict:
    return {"time": sec, "open": close - 0.0001, "high": close + 0.0002,
            "low": close - 0.0003, "close": close,
            "tick_volume": tick_vol, "real_volume": real_vol}


def test_pipe_safe_converts_unpicklable_namedtuple():
    """匿名 namedtuple（__module__=builtins）不可 pickle → 转 dict 回传（digits 丢失致精度污染 8 位的根因回归）"""
    from collections import namedtuple

    from gateway.market_sources.mt5_source import _pipe_safe

    Info = namedtuple("SymbolInfo", ["digits", "visible"])
    Info.__module__ = "builtins"   # MetaTrader5 包 C 层构造的真实形态
    assert _pipe_safe(Info(5, True)) == {"digits": 5, "visible": True}
    assert _pipe_safe([Info(3, True)]) == [{"digits": 3, "visible": True}]
    assert _pipe_safe(None) is None and _pipe_safe(True) is True and _pipe_safe(1.5) == 1.5


def test_timeframe_map_covers_frontend():
    # 前端常用周期均有映射；3d MT5 无对应周期
    for tf in ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]:
        assert tf in TIMEFRAME_MAP
    assert "3d" not in TIMEFRAME_MAP


def test_unavailable_when_terminal_not_connected():
    src = Mt5Source(driver=_FakeMt5(ok=False))
    assert src.available is False


def test_default_symbols_filtered_by_terminal():
    """默认品种过滤为终端实际存在的（经纪商命名可能有后缀），并缓存 digits"""
    src = Mt5Source(driver=_FakeMt5(symbols={"EURUSD"}))
    assert [s["symbol"] for s in src.default_symbols] == ["EURUSD"]
    assert src._digits["EURUSD"] == 5


async def test_fetch_klines_converts_rows_and_volume_fallback():
    rows = [_row(1786000000 + i * 60, 1.15 + i * 0.0001) for i in range(5)]
    drv = _FakeMt5(rows=rows)
    src = Mt5Source(driver=drv)
    bars = await src.fetch_klines("eurusd", "1m", limit=3)
    assert len(bars) == 3                               # 取最新 3 根
    assert bars[-1]["timestamp"] == (1786000000 + 4 * 60) * 1000   # 秒 → 毫秒
    assert bars[-1]["volume"] == 10.0                   # real_volume=0 → 用 tick_volume
    assert drv.selected == ["EURUSD"]                   # 惰性 symbol_select


async def test_fetch_klines_real_volume_preferred():
    rows = [_row(1786000000, 100.5, tick_vol=999, real_vol=42)]
    src = Mt5Source(driver=_FakeMt5(symbols={"BTCUSD"}, rows=rows))
    bars = await src.fetch_klines("BTCUSD", "1m", limit=10)
    assert bars[0]["volume"] == 42.0                    # 有 real_volume 优先


async def test_fetch_klines_end_time_uses_range_and_filters():
    """翻页加深（end_time）：走 copy_rates_range 且过滤 ts > end_time"""
    rows = [_row(1786000000 + i * 60, 1.15) for i in range(10)]
    drv = _FakeMt5(rows=rows)
    src = Mt5Source(driver=drv)
    end = (1786000000 + 7 * 60) * 1000
    bars = await src.fetch_klines("EURUSD", "1m", limit=5, end_time=end)
    assert len(drv.range_calls) == 1                    # 走 range 查询
    assert all(b["timestamp"] <= end for b in bars)     # end_time（含）之前
    assert len(bars) == 5                               # limit 截取
    assert bars[-1]["timestamp"] == end


async def test_fetch_klines_unsupported_timeframe():
    src = Mt5Source(driver=_FakeMt5())
    with pytest.raises(ValueError):
        await src.fetch_klines("EURUSD", "3d", limit=10)


async def test_price_precision_from_digits_not_derived():
    """精度铁律：symbol_info().digits（订阅到的市场元数据）优先于价格推导"""
    rows = [_row(1786000000, 1.15)]                     # 价格只能推出 2 位
    src = Mt5Source(driver=_FakeMt5(digits=5, rows=rows))
    await src.fetch_klines("EURUSD", "1m", limit=1)
    assert src.price_precision("EURUSD") == 5           # digits 权威


async def test_fetch_ticker_mid_and_stats():
    tick = {"bid": 1.15578, "ask": 1.15590, "time": 0}
    h1 = [_row(1786000000 + i * 3600, 1.15 + i * 0.0001) for i in range(25)]
    d1 = [_row(1785900000, 1.15), _row(1786000000, 1.155)]
    drv = _FakeMt5(rows=h1, tick=tick)

    # 按 timeframe 区分返回（fetch_ticker 依次取 1h/1d）
    def rates_by_tf(symbol, timeframe, start_pos, count):
        if timeframe == TIMEFRAME_MAP["1d"]:
            return d1
        return h1[-count:]

    drv.copy_rates_from_pos = rates_by_tf
    src = Mt5Source(driver=drv)
    t = await src.fetch_ticker("EURUSD")
    assert t["last_price"] == pytest.approx(1.15584)    # bid/ask Decimal 中点
    assert t["bid"] == pytest.approx(1.15578)
    assert t["price_change_pct"] == pytest.approx((1.15584 / 1.15 - 1) * 100)
    assert t["high_24h"] == pytest.approx(max(r["high"] for r in h1))
    assert t["low_24h"] == pytest.approx(min(r["low"] for r in h1))


async def test_fetch_ticker_fallback_to_candles():
    """无 tick 报价（休市/断连）：最新 M1 收盘构造 ticker"""
    m1 = [_row(1786000000, 1.1532)]
    drv = _FakeMt5(rows=m1, tick=None)
    src = Mt5Source(driver=drv)
    t = await src.fetch_ticker("EURUSD")
    assert t["last_price"] == pytest.approx(1.1532)
    assert t["ask"] == 0.0


async def test_reconnect_on_connection_lost():
    """终端断连（驱动返回 None）→ 重连；失败后冷却内不重复尝试"""
    drv = _FakeMt5()
    src = Mt5Source(driver=drv)
    assert src.available is True
    drv.ok = False
    src._try_reconnect()
    assert src.available is False and drv.shutdown_calls == 1
    # 冷却内再调不触发 shutdown/initialize
    src._try_reconnect()
    assert drv.shutdown_calls == 1
    # 冷却过后重试成功
    src._last_reconnect_at = 0.0
    drv.ok = True
    src._try_reconnect()
    assert src.available is True


# ─── 全量品种目录：path 资产分类 + trade_mode 过滤 ───


async def test_list_symbols_classify_by_path():
    """path 顶层目录归资产类别；Commodities 二级细分；非 FULL 不可交易品种过滤"""
    catalog = [
        {"name": "EURUSD", "description": "Euro vs US Dollar", "path": "Forex\\EURUSD", "trade_mode": 4},
        {"name": "XAUUSD", "description": "Gold vs US Dollar", "path": "Commodities\\Metals\\XAUUSD", "trade_mode": 4},
        {"name": "XTIUSD", "description": "Crude Oil", "path": "Commodities\\Energies\\Energies Spot\\XTIUSD", "trade_mode": 4},
        {"name": "US500", "description": "S&P 500", "path": "Indices\\Indices Spot\\Major Spot Indices\\US500", "trade_mode": 4},
        {"name": "BTCUSD", "description": "Bitcoin", "path": "Crypto\\BTCUSD", "trade_mode": 4},
        {"name": "AAPL.NASDAQ", "description": "Apple", "path": "Stock CFD's\\NASDAQ\\AAPL.NASDAQ", "trade_mode": 4},
        {"name": "UST10Y_U6", "description": "US 10Y Bond", "path": "Bonds CFDs\\UST10Y_U6", "trade_mode": 4},
        {"name": "CLOSED", "description": "closed", "path": "Forex\\CLOSED", "trade_mode": 0},
    ]
    src = Mt5Source(driver=_FakeMt5(catalog=catalog))
    rows = await src.list_symbols()
    by_type = {r["symbol"]: r["type"] for r in rows}
    assert by_type == {
        "EURUSD": "forex", "XAUUSD": "metal", "XTIUSD": "commodity",
        "US500": "index", "BTCUSD": "crypto", "AAPL.NASDAQ": "stock",
        "UST10Y_U6": "bond",
    }
    assert next(r for r in rows if r["symbol"] == "XAUUSD")["name"] == "Gold vs US Dollar"


async def test_list_symbols_fallback_to_defaults():
    """终端不可用（symbols_get 返回 None）→ 回退插件默认品种（带资产类别；code 缺省同 symbol）"""
    src = Mt5Source(driver=_FakeMt5(catalog=None))
    rows = await src.list_symbols()
    assert rows == [
        {"symbol": "EURUSD", "name": "EUR/USD", "type": "forex", "code": "EURUSD"},
        {"symbol": "GBPUSD", "name": "GBP/USD", "type": "forex", "code": "GBPUSD"},
    ]

