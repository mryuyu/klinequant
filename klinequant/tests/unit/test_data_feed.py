"""Mt5DataFeed / BacktestDataFeed 数据源单测（fake 驱动注入，不依赖真实终端）

覆盖：
  - M2 修复：新 bar 出现时用 rows[0]（刚收盘那根的终端终值）回填本地 bars[-1]
    的陈旧快照，避免丢失收盘前最后 ≤poll_interval 的 H/L 极值。
  - M1 field 语义：版本 key 按 field 分离——基础 key 表任意变化（含盘中 forming），
    ``{base}#bar`` 仅收盘时 bump；``is_changing(k, "timestamp")`` 据此区分收盘与盘中。
"""
import threading
import time

from strategy.sdk.api import KqApi
from strategy.sdk.backtest_feed import BacktestDataFeed
from strategy.sdk.data_feed import Mt5DataFeed


class _FakeDriver:
    """驱动 fake：copy_rates_from_pos 回传预置 rows（time=epoch 秒）"""

    def __init__(self, rows):
        self._rows = rows

    def symbol_select(self, symbol, enable=True):
        return True

    def symbol_info_tick(self, symbol):
        return None

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count, timeout=None):
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


# ─── M1：is_changing 的 field 语义（收盘 vs 盘中）───


def test_resolve_key_maps_close_field_to_bar_key():
    """M1：field ∈ 收盘字段(timestamp/time/datetime) → `{base}#bar`；
    其余 field/None → 基础 key；tick 类 key 忽略 field。"""
    feed = _feed([_row(100, high=1.1, low=1.0, close=1.05)])
    assert feed._resolve_key("EURUSD/1m") == "EURUSD/1m"
    assert feed._resolve_key("EURUSD/1m", "close") == "EURUSD/1m"
    for f in ("timestamp", "time", "datetime"):
        assert feed._resolve_key("EURUSD/1m", f) == "EURUSD/1m#bar"
    # bars 列表 + 收盘 field 亦映射到 #bar
    bars = [{"symbol": "EURUSD", "period": "1m", "timestamp": 100_000}]
    assert feed._resolve_key(bars, "timestamp") == "EURUSD/1m#bar"
    # tick 类 key 无收盘概念，field 忽略（不映射到 #bar）
    assert feed._resolve_key("tick/EURUSD", "timestamp") == "tick/EURUSD"


def test_poll_bars_forming_bumps_base_only():
    """M1：forming 盘中更新（同 timestamp）只 bump 基础 key，#bar 收盘 key 不动。"""
    feed = _feed([
        _row(100, high=1.4, low=0.7, close=1.0),
        _row(100, high=1.6, low=0.6, close=1.05),
    ])
    feed._bars["EURUSD/1m"] = [
        {"symbol": "EURUSD", "period": "1m", "timestamp": 100_000,
         "open": 1.0, "high": 1.3, "low": 0.8, "close": 0.95, "volume": 5.0},
    ]
    base0 = feed._versions.get("EURUSD/1m", 0)
    bar0 = feed._versions.get("EURUSD/1m#bar", 0)
    assert feed._poll_bars("EURUSD", "1m") is True
    assert feed._versions["EURUSD/1m"] == base0 + 1          # 基础 key bump
    assert feed._versions.get("EURUSD/1m#bar", 0) == bar0    # #bar 未 bump（未收盘）


def test_poll_bars_new_bar_bumps_base_and_bar_key():
    """M1：新 bar 出现（timestamp 变化 = 上一根收盘）同时 bump 基础 key 与 #bar。"""
    feed = _feed([
        _row(100, high=1.5, low=0.5, close=1.1),
        _row(160, high=1.2, low=1.0, close=1.15),
    ])
    feed._bars["EURUSD/1m"] = [
        {"symbol": "EURUSD", "period": "1m", "timestamp": 100_000,
         "open": 1.0, "high": 1.3, "low": 0.8, "close": 0.95, "volume": 5.0},
    ]
    base0 = feed._versions.get("EURUSD/1m", 0)
    bar0 = feed._versions.get("EURUSD/1m#bar", 0)
    assert feed._poll_bars("EURUSD", "1m") is True
    assert feed._versions["EURUSD/1m"] == base0 + 1
    assert feed._versions["EURUSD/1m#bar"] == bar0 + 1       # 收盘 key 也 bump


def test_is_changing_separates_forming_from_close():
    """M1 验收：仅 forming 变化 → is_changing(k) True 且 is_changing(k,'timestamp') False；
    新 bar 收盘 → 两者均 True。"""
    driver = _FakeDriver([_row(100, high=1.4, low=0.7, close=1.0)])
    feed = Mt5DataFeed(driver, symbols=["EURUSD"], periods=["1m"])
    feed._bars["EURUSD/1m"] = [
        {"symbol": "EURUSD", "period": "1m", "timestamp": 100_000,
         "open": 1.0, "high": 1.3, "low": 0.8, "close": 0.95, "volume": 5.0},
    ]
    bars = feed._bars["EURUSD/1m"]
    feed._snapshot_versions = dict(feed._versions)   # 模拟 wait_update 刚返回的基线

    # ① 仅 forming 盘中变化（同 t=100，close 0.95→1.05）
    driver._rows = [_row(100, high=1.4, low=0.7, close=1.0),
                    _row(100, high=1.6, low=0.6, close=1.05)]
    assert feed._poll_bars("EURUSD", "1m") is True
    assert feed.is_changing(bars) is True                 # 基础 key 变（盘中）
    assert feed.is_changing(bars, "timestamp") is False   # 未收盘 → #bar 未变

    # ② 新 bar 出现（t=100 收盘 + t=160 forming）→ #bar 也变
    driver._rows = [_row(100, high=1.6, low=0.6, close=1.05),
                    _row(160, high=1.2, low=1.0, close=1.15)]
    assert feed._poll_bars("EURUSD", "1m") is True
    assert feed.is_changing(bars) is True
    assert feed.is_changing(bars, "timestamp") is True


def test_backtest_feed_every_bar_is_close():
    """M1 同构：回测每根皆收盘 bar → is_changing(k) 与 is_changing(k,'timestamp') 每轮均 True。"""
    bars = [{"symbol": "TESTUSD", "period": "1m",
             "timestamp": 1_600_000_000_000 + i * 60_000,
             "open": 1.1, "high": 1.11, "low": 1.09, "close": 1.1, "volume": 1.0}
            for i in range(5)]
    feed = BacktestDataFeed({"TESTUSD": bars}, "1m")
    assert feed._resolve_key("TESTUSD/1m", "timestamp") == "TESTUSD/1m#bar"
    assert feed.wait_update() is True
    k = feed.latest_bars("TESTUSD", "1m")
    assert feed.is_changing(k) is True
    assert feed.is_changing(k, "timestamp") is True
    assert feed.is_changing(k, "datetime") is True


# ─── M3/M4-a/M4-b：唤醒竞态 + per-api 快照隔离 + per-symbol 事件 ───


def test_wait_and_snapshot_no_lost_wakeup():
    """M3：等待期间的 bump+set 不被吞——同轮立即返回，且快照为 wait 起始基线。

    clear→snapshot→wait 顺序保证：任何并发 set 要么已被快照收录，要么令
    wait 立即返回（is_changing 判 True），绝不因 clear 吞掉而延迟一整轮。
    """
    feed = Mt5DataFeed(_FakeDriver([]), symbols=["EURUSD"], periods=["1m"])
    feed._running = True
    with feed._lock:
        feed._bump_version("EURUSD/1m")   # 基线 version=1
    result = {}

    def waiter():
        got, snap = feed.wait_and_snapshot(deadline=5.0, symbol="EURUSD")
        result["got"] = got
        result["snap"] = snap

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.15)                      # 确保已过 clear+snapshot，阻塞在 wait
    with feed._lock:
        feed._bump_version("EURUSD/1m")   # 等待期间 bump→2
    feed._event_for("EURUSD").set()
    t.join(timeout=3.0)
    assert result["got"] is True
    # 快照为 wait 起始基线(=1)，不含等待期间的 bump(=2) → is_changing 判 True（不漏事件）
    assert result["snap"].get("EURUSD/1m") == 1
    assert feed.current_version("EURUSD/1m") == 2


def test_per_symbol_event_only_wakes_target():
    """M4-b：品种级事件隔离——唤醒 EURUSD 不会惊群唤醒等待 GBPUSD 的线程。"""
    feed = Mt5DataFeed(_FakeDriver([]), symbols=["EURUSD", "GBPUSD"], periods=["1m"])
    feed._running = True
    ev_eur = feed._event_for("EURUSD")
    ev_gbp = feed._event_for("GBPUSD")
    assert ev_eur is not ev_gbp
    assert ev_eur is not feed._event      # 品种级事件 != 全局事件

    result = {}

    def waiter():
        got, _snap = feed.wait_and_snapshot(deadline=3.0, symbol="GBPUSD")
        result["got"] = got

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.15)
    ev_eur.set()                          # 唤醒错品种 → GBPUSD 等待者不应返回
    time.sleep(0.25)
    assert "got" not in result            # 仍阻塞（未被惊群）
    ev_gbp.set()                          # 唤醒本品种
    t.join(timeout=3.0)
    assert result["got"] is True


class _CtrlFeed:
    """可控 feed：wait_and_snapshot 立即返回当前版本快照（供 per-api 隔离测试）。"""

    def __init__(self):
        self._versions: dict[str, int] = {}

    def wait_and_snapshot(self, deadline=None, symbol=None):
        return True, dict(self._versions)

    def resolve_key(self, obj, field=None):
        return obj if isinstance(obj, str) else None

    def current_version(self, key):
        return self._versions.get(key, 0)


def _mk_api(feed, symbol):
    return KqApi(symbol=symbol, period="1m", tag=f"t:{symbol}", specs={},
                 ledger=None, resolver=None, executor=None, feed=feed)


def test_per_api_snapshot_isolation():
    """M4-a：两 KqApi 共享一个 feed，各自持有独立 is_changing 基线，互不覆盖。

    旧版 feed 级单一 snapshot_versions 下，后一次 wait_update 会覆盖前者的基线，
    导致先 wait 的 api 漏判变化（拓扑 Z 多线程必现）。
    """
    feed = _CtrlFeed()
    api_a = _mk_api(feed, "EURUSD")
    api_b = _mk_api(feed, "GBPUSD")

    api_a.wait_update(0.0)              # A 基线：{} (EURUSD/1m=0)
    feed._versions["EURUSD/1m"] = 1     # EURUSD 更新
    api_b.wait_update(0.0)              # B 基线：{EURUSD/1m:1}

    # A 自其上次 wait 后 EURUSD 变了 → True；B 自其上次 wait 后未变 → False
    assert api_a.is_changing("EURUSD/1m") is True
    assert api_b.is_changing("EURUSD/1m") is False
