"""Phase R4 策略语义状态持久化单元测试。

覆盖《SDK 阶段实施规划 v1.3》R4 验收点：
  - StateStore 是 dict 子类：变更钩子标脏 + debounce 落盘，读操作不触发落盘
  - 后端可插拔：SqliteStateBackend（落盘 + 重开存活）/ InMemoryStateBackend（回测/单测）
  - 自动 load/save 为主干：LiveRunner 启动预填充、退出落盘；策略级共享一份（全品种同一 StateStore）
  - api.is_resumed()：区分冷启动 vs 崩溃恢复（供策略决定是否先 flatten）
  - 红线兜底：backend=None → 纯内存零落盘（回测/--no-state 零破坏）
  - JSON 序列化：int/float/str/bool/None/list/dict 精确往返
"""
from decimal import Decimal

from protocol.types import SymbolInfo
from strategy.sdk.api import KqApi
from strategy.sdk.live_runner import LiveRunner
from strategy.sdk.state_store import (
    InMemoryStateBackend,
    SqliteStateBackend,
    StateStore,
)

# ─── 脚手架 ───


def _fx_spec(symbol="EURUSD"):
    return SymbolInfo(
        symbol=symbol, exchange="mt5", base_currency="EUR", quote_currency="USD",
        price_precision=5, qty_precision=2, min_qty=Decimal("0.01"),
        min_notional=Decimal("0"), tick_size=Decimal("0.00001"),
        market_type="FX", qty_unit="LOT", qty_step=Decimal("0.01"),
        qty_max=Decimal("100"), pip_size=Decimal("0.0001"),
        contract_multiplier=Decimal("100000"), can_short=True,
        t_plus_n=0, close_priority="net",
    )


class _SpyBackend:
    """记录 save 调用的后端桩（验证 debounce / 读不落盘 / 脏门控）。"""

    def __init__(self, seeded=None):
        self.saved = []
        self._seeded = seeded

    def load(self, key):
        return dict(self._seeded) if self._seeded is not None else None

    def save(self, key, payload):
        self.saved.append(dict(payload))

    def close(self):
        pass


class _NullExec:
    def submit(self, spec):
        return None

    def cancel(self, ticket, symbol, magic=None):
        return True

    def query_positions(self, symbol="", magic=None):
        return []

    def query_orders(self, symbol="", magic=None):
        return []

    def query_account(self):
        return {"balance": 1.0, "margin_free": 1.0, "margin": 0.0, "profit": 0.0}

    def query_order_outcome(self, client_order_id, symbol=""):
        return None


class _NullFeed:
    def __init__(self):
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def wait_update(self, deadline=None):
        return False

    def latest_bars(self, symbol, period, count):
        return []

    def latest_tick(self, symbol):
        return None

    def is_changing(self, obj, field=None):
        return False

    def now_ms(self):
        return 0


class _NullBackend:
    """最小 MarketBackend：够 LiveRunner 全流程跑通（无持仓、无挂单、立即退出）。"""

    account_name = "acct"
    account_magic = None

    def __init__(self):
        self.exec = _NullExec()
        self.feed = _NullFeed()
        self.shutdown_called = False

    def connect(self):
        pass

    def load_specs(self, symbols):
        return {s: _fx_spec(s) for s in symbols}

    def make_executor(self):
        return self.exec

    def make_feed(self, symbols, periods, poll_interval, bar_count):
        return self.feed

    def reconcile_positions(self, executor, ledger, symbols, tag, magic=None):
        pass

    def shutdown(self):
        self.shutdown_called = True


def _make_api(state=None):
    """构造只用于 state/is_resumed 断言的 KqApi（其余依赖传 None，__init__ 不触碰）。"""
    return KqApi(
        symbol="EURUSD", period="1h", tag="macd:1h", specs={},
        ledger=None, resolver=None, executor=None, feed=None, state=state,
    )


# ═══════════════════════════════════════════
# StateStore：dict 语义 + 脏标记 + debounce
# ═══════════════════════════════════════════

def test_statestore_behaves_like_dict():
    st = StateStore()
    st["a"] = 1
    st.update({"b": 2, "c": 3})
    assert st.get("a") == 1 and st["b"] == 2 and len(st) == 3
    assert st.pop("c") == 3 and "c" not in st
    assert st.setdefault("d", 4) == 4
    del st["d"]
    assert "d" not in st
    st.clear()
    assert len(st) == 0
    assert st.is_resumed() is False


def test_statestore_no_backend_pure_memory():
    """backend=None：变更照常、save/load no-op、不落盘、is_resumed 恒 False（回测零破坏）"""
    st = StateStore()
    st["x"] = 1
    st.save()                     # no-op，不抛
    assert st.load() is st        # 返回 self，no-op
    assert st.is_resumed() is False
    assert st["x"] == 1


def test_statestore_first_write_flushes_immediately():
    """首次变更立即落盘（_last_flush==0 哨兵），不 defer 唯一一次写"""
    spy = _SpyBackend()
    st = StateStore(backend=spy, key="k", debounce=1000.0)
    st["a"] = 1
    assert len(spy.saved) == 1
    assert spy.saved[-1] == {"a": 1}


def test_statestore_debounce_defers_second_write():
    """debounce 窗内的后续变更被节流（标脏不落盘），显式 save 才补落"""
    spy = _SpyBackend()
    st = StateStore(backend=spy, key="k", debounce=1000.0)
    st["a"] = 1                   # 首次 → 立即落盘
    assert len(spy.saved) == 1
    st["b"] = 2                   # 窗内 → defer
    assert len(spy.saved) == 1
    assert st._dirty is True
    st.save()                     # 显式 → 落盘
    assert len(spy.saved) == 2
    assert spy.saved[-1] == {"a": 1, "b": 2}


def test_statestore_save_noop_when_clean():
    """脏门控：未变更时 save 不写后端"""
    spy = _SpyBackend()
    st = StateStore(backend=spy, key="k", debounce=1000.0)
    st.save()
    assert spy.saved == []


def test_statestore_read_does_not_flush():
    """读操作（get/[]/in/keys）不触发落盘"""
    spy = _SpyBackend()
    st = StateStore(backend=spy, key="k", debounce=1000.0)
    st["a"] = 1                   # 首次落盘
    n = len(spy.saved)
    _ = st.get("a")
    _ = st["a"]
    _ = "a" in st
    _ = list(st.keys())
    _ = list(st.items())
    assert len(spy.saved) == n


def test_statestore_setdefault_marks_dirty_only_on_insert():
    spy = _SpyBackend()
    st = StateStore(backend=spy, key="k", debounce=1000.0)
    assert st.setdefault("a", 1) == 1     # 插入 → 脏 + 首次落盘
    assert len(spy.saved) == 1
    assert st.setdefault("a", 99) == 1    # 已存在 → 不脏、不落盘
    assert len(spy.saved) == 1
    assert st._dirty is False


def test_statestore_clear_persists_empty():
    b = InMemoryStateBackend()
    st = StateStore(backend=b, key="k", debounce=0.0)
    st["a"] = 1
    st.clear()
    assert len(st) == 0
    assert b.load("k") == {}


def test_statestore_load_missing_not_resumed():
    b = InMemoryStateBackend()
    st = StateStore(backend=b, key="nope")
    st.load()
    assert st.is_resumed() is False
    assert len(st) == 0


def test_statestore_load_replaces_local_content():
    """load 用磁盘快照整体替换本地内容（重启预填充语义）"""
    b = InMemoryStateBackend()
    b.save("k", {"from_disk": 1})
    st = StateStore(backend=b, key="k")
    dict.__setitem__(st, "local_only", 99)   # 绕过变更钩子，模拟构造期残留
    st.load()
    assert st.get("from_disk") == 1
    assert "local_only" not in st
    assert st.is_resumed() is True


def test_statestore_nested_roundtrip_inmemory():
    b = InMemoryStateBackend()
    st = StateStore(backend=b, key="k", debounce=0.0)
    payload = {"int": 7, "float": 1.5, "str": "up", "bool": True, "none": None,
               "list": [1, 2, 3], "dict": {"a": {"b": [4, 5]}}}
    st.update(payload)
    st2 = StateStore(backend=b, key="k")
    st2.load()
    assert st2 == payload
    assert st2.is_resumed() is True


# ═══════════════════════════════════════════
# SqliteStateBackend：落盘 + 重开存活 + JSON 类型
# ═══════════════════════════════════════════

def test_sqlite_backend_roundtrip(tmp_path):
    b = SqliteStateBackend(tmp_path / "state.db")
    assert b.load("k") is None
    b.save("k", {"a": 1, "nested": {"x": [1, 2, 3]}})
    assert b.load("k") == {"a": 1, "nested": {"x": [1, 2, 3]}}
    b.close()


def test_sqlite_backend_persists_across_reopen(tmp_path):
    db = tmp_path / "state.db"
    b1 = SqliteStateBackend(db)
    b1.save("acct:macd:1m", {"counter": 7})
    b1.close()
    b2 = SqliteStateBackend(db)             # 模拟重启重开同一文件
    assert b2.load("acct:macd:1m") == {"counter": 7}
    b2.close()


def test_sqlite_backend_overwrites(tmp_path):
    b = SqliteStateBackend(tmp_path / "state.db")
    b.save("k", {"v": 1})
    b.save("k", {"v": 2})
    assert b.load("k") == {"v": 2}
    b.close()


def test_statestore_sqlite_json_types(tmp_path):
    """SQLite 后端经 JSON 序列化：常见标量/容器精确往返"""
    db = tmp_path / "state.db"
    b = SqliteStateBackend(db)
    st = StateStore(backend=b, key="k", debounce=0.0)
    st.update({"int": 7, "float": 1.5, "str": "up", "bool": True, "none": None,
               "list": [1, 2, 3], "dict": {"a": {"b": [4, 5]}}})
    b.close()
    b2 = SqliteStateBackend(db)
    st2 = StateStore(backend=b2, key="k")
    st2.load()
    assert st2["int"] == 7 and st2["float"] == 1.5 and st2["str"] == "up"
    assert st2["bool"] is True and st2["none"] is None
    assert st2["list"] == [1, 2, 3] and st2["dict"] == {"a": {"b": [4, 5]}}
    b2.close()


def test_statestore_sqlite_end_to_end_resume(tmp_path):
    """完整链路：写→落盘→重开→load→is_resumed，模拟崩溃重启恢复语义状态"""
    db = tmp_path / "state.db"
    b = SqliteStateBackend(db, synchronous="FULL")
    st = StateStore(backend=b, key="acct:s:1h", debounce=0.0)
    st["EURUSD:1h:trend"] = "up"
    st["add_count"] = 3
    b.close()
    b2 = SqliteStateBackend(db)
    st2 = StateStore(backend=b2, key="acct:s:1h")
    st2.load()
    assert st2.is_resumed() is True
    assert st2["EURUSD:1h:trend"] == "up"
    assert st2["add_count"] == 3
    b2.close()


# ═══════════════════════════════════════════
# InMemoryStateBackend：快照隔离
# ═══════════════════════════════════════════

def test_inmemory_backend_returns_isolated_copy():
    b = InMemoryStateBackend()
    b.save("k", {"a": 1})
    got = b.load("k")
    got["a"] = 999                          # 改返回副本
    assert b.load("k") == {"a": 1}          # 存储不受污染


# ═══════════════════════════════════════════
# KqApi 集成：state property + is_resumed 委托
# ═══════════════════════════════════════════

def test_kqapi_state_returns_injected_store():
    st = StateStore()
    api = _make_api(st)
    assert api.state is st
    api.state["x"] = 1
    assert st["x"] == 1


def test_kqapi_default_state_is_memory_store():
    """未注入 state（回测/单测）：api.state 仍是可用 StateStore，纯内存"""
    api = _make_api()
    assert isinstance(api.state, StateStore)
    api.state["x"] = 1
    assert api.state.get("x") == 1
    assert api.is_resumed() is False


def test_kqapi_is_resumed_delegates():
    b = InMemoryStateBackend()
    b.save("k", {"prior": 1})
    st = StateStore(backend=b, key="k")
    st.load()
    api = _make_api(st)
    assert api.is_resumed() is True


# ═══════════════════════════════════════════
# LiveRunner 集成：策略级共享 + 自动 load/save
# ═══════════════════════════════════════════

def test_runner_shares_state_across_symbols():
    """策略级共享一份：多品种写各自键，退出后同一快照含全部（证明共享同一 StateStore）"""
    store_backend = InMemoryStateBackend()
    backend = _NullBackend()

    def strategy(api):
        api.state[f"{api._symbol}:seen"] = True

    runner = LiveRunner(
        backend, symbols=["EURUSD", "GBPUSD"], period="1m",
        strategy_fn=strategy, tag="macd:1m", account_name="acct",
        state_backend=store_backend, state_debounce=0.0,
    )
    runner.run()
    saved = store_backend.load("acct:macd:1m")
    assert saved is not None
    assert saved.get("EURUSD:seen") is True
    assert saved.get("GBPUSD:seen") is True
    assert backend.shutdown_called


def test_runner_autoloads_resumed_state():
    """启动预填充：上一轮快照被 load，策略见 is_resumed=True + 旧值"""
    store_backend = InMemoryStateBackend()
    store_backend.save("acct:macd:1m", {"prior": 42, "trend": "up"})
    backend = _NullBackend()
    seen = {}

    def strategy(api):
        seen["resumed"] = api.is_resumed()
        seen["prior"] = api.state.get("prior")
        seen["trend"] = api.state.get("trend")

    runner = LiveRunner(
        backend, symbols=["EURUSD"], period="1m",
        strategy_fn=strategy, tag="macd:1m", account_name="acct",
        state_backend=store_backend, state_debounce=0.0,
    )
    runner.run()
    assert seen["resumed"] is True
    assert seen["prior"] == 42
    assert seen["trend"] == "up"


def test_runner_cold_start_not_resumed_and_saves():
    """冷启动：无历史快照 → is_resumed=False；退出自动 save 新状态"""
    store_backend = InMemoryStateBackend()
    backend = _NullBackend()
    seen = {}

    def strategy(api):
        seen["resumed"] = api.is_resumed()
        api.state["fresh"] = 1

    runner = LiveRunner(
        backend, symbols=["EURUSD"], period="1m",
        strategy_fn=strategy, tag="macd:1m", account_name="acct",
        state_backend=store_backend, state_debounce=0.0,
    )
    runner.run()
    assert seen["resumed"] is False
    assert store_backend.load("acct:macd:1m") == {"fresh": 1}


def test_runner_without_state_backend_still_runs():
    """无 state_backend（--no-state / 旧调用）：纯内存、零落盘、不报错"""
    backend = _NullBackend()
    seen = {}

    def strategy(api):
        seen["resumed"] = api.is_resumed()
        api.state["x"] = 1
        seen["x"] = api.state.get("x")

    runner = LiveRunner(
        backend, symbols=["EURUSD"], period="1m",
        strategy_fn=strategy, tag="macd:1m", account_name="acct",
    )
    runner.run()
    assert seen["resumed"] is False
    assert seen["x"] == 1
    assert backend.shutdown_called
