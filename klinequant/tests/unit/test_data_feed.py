"""Mt5DataFeed._poll_bars 单测（fake 驱动注入，不依赖真实终端）

覆盖 M2 修复：新 bar 出现时，用 rows[0]（刚收盘那根的终端终值）回填本地
bars[-1] 的陈旧快照，避免丢失收盘前最后 ≤poll_interval 的 H/L 极值。
"""
from strategy.sdk.data_feed import Mt5DataFeed


class _FakeDriver:
    """驱动 fake：copy_rates_from_pos 回传预置 rows（time=epoch 秒）"""

    def __init__(self, rows):
        self._rows = rows

    def symbol_select(self, symbol, enable=True):
        return True

    def symbol_info_tick(self, symbol):
        return None

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count):
        return self._rows[-count:]


def _row(sec: int, high: float, low: float, close: float) -> dict:
    return {
        "time": sec, "open": close, "high": high, "low": low,
        "close": close, "tick_volume": 10, "real_volume": 0,
    }


def _feed(rows):
    return Mt5DataFeed(_FakeDriver(rows), symbols=["EURUSD"], periods=["1m"])


def test_poll_bars_backfills_closed_bar_terminal_value():
    """M2：新 bar 到达时，刚收盘那根须回填终端终值（含更宽的 H/L 极值）"""
    # rows[0]=刚收盘 bar(t=100) 的终端终值，high/low 比本地陈旧快照更宽；
    # rows[1]=新出现的 forming bar(t=160)
    feed = _feed([
        _row(100, high=1.5, low=0.5, close=1.1),
        _row(160, high=1.2, low=1.0, close=1.15),
    ])
    # 本地 bars[-1] 是 t=100 的陈旧快照（收盘前最后一次轮询，极值偏窄）
    feed._bars["EURUSD/1m"] = [
        {"symbol": "EURUSD", "period": "1m", "timestamp": 100_000,
         "open": 1.0, "high": 1.3, "low": 0.8, "close": 0.95, "volume": 5.0},
    ]

    changed = feed._poll_bars("EURUSD", "1m")
    assert changed is True

    bars = feed._bars["EURUSD/1m"]
    assert len(bars) == 2
    # 回填：t=100 那根取终端终值（high=1.5 / low=0.5），而非陈旧快照的 1.3 / 0.8
    assert bars[-2]["timestamp"] == 100_000
    assert bars[-2]["high"] == 1.5
    assert bars[-2]["low"] == 0.5
    assert bars[-2]["close"] == 1.1
    # 新 bar 正常 append
    assert bars[-1]["timestamp"] == 160_000


def test_poll_bars_no_backfill_on_timestamp_gap():
    """保护：rows[0] 与本地 bars[-1] 时间戳不一致（跳空）时不误回填，仅 append"""
    feed = _feed([
        _row(40, high=9.9, low=0.1, close=5.0),   # 与本地 last_bar 时间戳不同
        _row(160, high=1.2, low=1.0, close=1.15),
    ])
    feed._bars["EURUSD/1m"] = [
        {"symbol": "EURUSD", "period": "1m", "timestamp": 100_000,
         "open": 1.0, "high": 1.3, "low": 0.8, "close": 0.95, "volume": 5.0},
    ]

    changed = feed._poll_bars("EURUSD", "1m")
    assert changed is True

    bars = feed._bars["EURUSD/1m"]
    # last_bar(t=100) 保持原值，未被 rows[0](t=40) 覆盖
    assert bars[-2]["timestamp"] == 100_000
    assert bars[-2]["high"] == 1.3
    assert bars[-2]["low"] == 0.8
    assert bars[-1]["timestamp"] == 160_000


def test_poll_bars_forming_update_no_backfill():
    """盘中 forming bar 更新（同 timestamp）走原路径，不触发回填逻辑"""
    feed = _feed([
        _row(100, high=1.4, low=0.7, close=1.0),
        _row(100, high=1.6, low=0.6, close=1.05),  # 同 t，forming 变化
    ])
    feed._bars["EURUSD/1m"] = [
        {"symbol": "EURUSD", "period": "1m", "timestamp": 100_000,
         "open": 1.0, "high": 1.3, "low": 0.8, "close": 0.95, "volume": 5.0},
    ]

    changed = feed._poll_bars("EURUSD", "1m")
    assert changed is True

    bars = feed._bars["EURUSD/1m"]
    assert len(bars) == 1  # 未新增 bar
    assert bars[-1]["high"] == 1.6
    assert bars[-1]["close"] == 1.05
