"""Phase R3 启动恢复单元测试。

覆盖《SDK 阶段实施规划 v1.3》R3 验收点：
  - recover 四分支：已成交(FILLED) / 仍挂着(OPEN→IN_FLIGHT) / 查无此单(None→DEAD) / venue 不可达
  - journal 为 None（回测/--no-journal）→ _recover 零副作用（不探测、不抛）
  - 恢复期 venue 掉线（query_order_outcome 抛）→ 中止启动（不带病起）
  - pending 按 (account, tag) 过滤：只收敛本策略意图
  - reconcile 按 magic 隔离 + 挂单 in_flight 恢复（缺口2，凭 client_order_id 去重不双计）
  - Mt5Executor.query_order_outcome：OPEN / FILLED(持仓) / FILLED(历史成交) / None
  - Mt5Executor.query_positions/query_orders 按 magic 客户端过滤
"""
from decimal import Decimal

import pytest

from core.trade_engine.executors.mt5_executor import Mt5Executor
from core.trade_engine.ledger import ExposureLedger
from protocol.types import Offset, OrderSide
from strategy.sdk.backend import _reconcile_net_positions
from strategy.sdk.live_runner import LiveRunner
from strategy.sdk.order_journal import (
    STATE_DEAD,
    STATE_IN_FLIGHT,
    InMemoryJournal,
)

# ─── 测试脚手架 ───


class _FakeBackend:
    """最小 backend（_recover 单测不触发 backend 调用，仅供 LiveRunner 构造）。"""


class _StubApi:
    """_recover 只写 api._order_tickets，用桩替代 KqApi。"""

    def __init__(self, symbol):
        self._symbol = symbol
        self._order_tickets = {}


class _RecoverExec:
    """恢复期执行器桩：query_account 探测 + query_order_outcome 反查。"""

    _UNSET = object()

    def __init__(self, outcomes=None, account=_UNSET, probe_exc=None, outcome_exc=None):
        self._outcomes = outcomes or {}
        # account=None 显式表示 venue 不可达（query_account 返 None）；缺省为可达
        self._account = {"balance": 1.0} if account is self._UNSET else account
        self._probe_exc = probe_exc
        self._outcome_exc = outcome_exc
        self.probe_calls = 0
        self.outcome_calls = []

    def query_account(self):
        self.probe_calls += 1
        if self._probe_exc is not None:
            raise self._probe_exc
        return self._account

    def query_order_outcome(self, client_order_id, symbol=""):
        self.outcome_calls.append((client_order_id, symbol))
        if self._outcome_exc is not None:
            raise self._outcome_exc
        return self._outcomes.get(client_order_id)   # 缺省 None = 查无此单


def _make_runner(executor, journal, *, symbols=("EURUSD",), tag="macd:1h",
                 account="acct", retries=1):
    runner = LiveRunner(
        _FakeBackend(), symbols=list(symbols), period="1h",
        strategy_fn=lambda api: None, tag=tag,
        account_name=account, journal=journal,
        recover_retries=retries, recover_backoff=0.0,
    )
    runner._executor = executor
    runner._ledger = ExposureLedger()
    runner._apis = {s: _StubApi(s) for s in symbols}
    return runner


def _pending_journal(coid, *, side="BUY", offset="OPEN", qty="0.10",
                     symbol="EURUSD", account="acct", tag="macd:1h"):
    """写一条 SUBMITTING（非终态）意图，模拟崩溃前 begin 已落盘。"""
    j = InMemoryJournal()
    j.begin(client_order_id=coid, account=account, tag=tag, symbol=symbol,
            side=side, offset=offset, qty=Decimal(qty))
    return j


# ─── recover 四分支 ───

def test_recover_filled_patches_ledger_and_journal():
    """已成交：补 ledger 持仓（volume+=signed、in_flight 净归零）+ journal FILLED + ticket"""
    j = _pending_journal("K1", qty="0.10")
    ex = _RecoverExec(outcomes={"K1": {
        "state": "FILLED", "ticket": 11,
        "filled_qty": Decimal("0.10"), "filled_price": Decimal("1.1000"),
    }})
    r = _make_runner(ex, j)
    r._recover()
    assert j.pending() == []                              # FILLED 终态离开 pending
    assert j._rows["K1"].status == "FILLED"
    pos = r._ledger.position("EURUSD", "macd:1h")
    assert pos.volume == Decimal("0.10")                  # BUY OPEN → +0.10
    assert pos.in_flight == Decimal("0")                  # accepted+filled 净在途归零
    assert r._apis["EURUSD"]._order_tickets["K1"] == 11


def test_recover_in_flight_restores_inflight_and_ticket():
    """仍挂着：补 in_flight + ticket，journal 落 IN_FLIGHT（仍 pending）"""
    j = _pending_journal("K2", qty="0.10")
    ex = _RecoverExec(outcomes={"K2": {"state": "OPEN", "ticket": 22}})
    r = _make_runner(ex, j)
    r._recover()
    pend = j.pending()
    assert len(pend) == 1
    assert pend[0].status == STATE_IN_FLIGHT and pend[0].ticket == 22
    pos = r._ledger.position("EURUSD", "macd:1h")
    assert pos.in_flight == Decimal("0.10")               # BUY OPEN 在途
    assert pos.volume == Decimal("0")
    assert r._apis["EURUSD"]._order_tickets["K2"] == 22


def test_recover_missing_marks_dead():
    """查无此单（venue 返 None）：标 DEAD 释放，ledger 不受污染"""
    j = _pending_journal("K3", qty="0.10")
    ex = _RecoverExec(outcomes={})                        # K3 缺省 → None
    r = _make_runner(ex, j)
    r._recover()
    assert j.pending() == []
    assert j._rows["K3"].status == STATE_DEAD
    pos = r._ledger.position("EURUSD", "macd:1h")
    assert pos.volume == Decimal("0") and pos.in_flight == Decimal("0")


def test_recover_explicit_dead_state():
    """venue 明确返 DEAD（撤单/过期）：同样标 DEAD 收敛"""
    j = _pending_journal("K4")
    ex = _RecoverExec(outcomes={"K4": {"state": "DEAD", "ticket": 0, "reason": "canceled"}})
    r = _make_runner(ex, j)
    r._recover()
    assert j.pending() == []
    assert j._rows["K4"].status == STATE_DEAD
    assert "canceled" in j._rows["K4"].reason


def test_recover_sell_close_fill_sign():
    """SELL/CLOSE 成交恢复：volume -= qty（带符号正确）"""
    j = _pending_journal("K5", side="SELL", offset="CLOSE", qty="0.10")
    ex = _RecoverExec(outcomes={"K5": {
        "state": "FILLED", "ticket": 55,
        "filled_qty": Decimal("0.10"), "filled_price": Decimal("1.0000"),
    }})
    r = _make_runner(ex, j)
    r._recover()
    assert r._ledger.position("EURUSD", "macd:1h").volume == Decimal("-0.10")


# ─── venue 不可达 ───

def test_recover_venue_unreachable_raises_and_stays_pending():
    """venue 不可达（query_account=None）：抛错中止启动，意图不收敛（不带病起）"""
    j = _pending_journal("K6")
    ex = _RecoverExec(account=None)
    r = _make_runner(ex, j, retries=1)
    with pytest.raises(RuntimeError, match="unreachable"):
        r._recover()
    assert len(j.pending()) == 1                          # 未收敛


def test_recover_venue_probe_retries_then_raises():
    """探测异常：重试到上限仍不可达 → 抛错"""
    j = _pending_journal("K7")
    ex = _RecoverExec(probe_exc=ConnectionError("down"))
    r = _make_runner(ex, j, retries=3)
    with pytest.raises(RuntimeError):
        r._recover()
    assert ex.probe_calls == 3                            # 重试到上限


def test_recover_outcome_query_raises_aborts():
    """探测通过但反查期 venue 掉线：抛出中止（不可信「查无此单」）"""
    j = _pending_journal("K8")
    ex = _RecoverExec(outcome_exc=ConnectionError("drop mid-recovery"))
    r = _make_runner(ex, j)
    with pytest.raises(RuntimeError, match="unreachable during recovery"):
        r._recover()


# ─── 零破坏 / 过滤 ───

def test_recover_no_journal_is_noop():
    """journal=None（回测/--no-journal）：不探测、不抛、零副作用"""
    ex = _RecoverExec()
    r = _make_runner(ex, journal=None)
    r._recover()
    assert ex.probe_calls == 0


def test_recover_clean_start_skips_probe():
    """journal 存在但无 pending：clean start，不探测 venue"""
    ex = _RecoverExec()
    r = _make_runner(ex, InMemoryJournal())
    r._recover()
    assert ex.probe_calls == 0


def test_recover_filters_by_tag():
    """pending 按 (account, tag) 过滤：只收敛本策略意图，他 tag 不动"""
    j = InMemoryJournal()
    j.begin(client_order_id="MINE", account="acct", tag="macd:1h", symbol="EURUSD",
            side="BUY", offset="OPEN", qty=Decimal("0.1"))
    j.begin(client_order_id="OTHER", account="acct", tag="other:5m", symbol="EURUSD",
            side="BUY", offset="OPEN", qty=Decimal("0.1"))
    ex = _RecoverExec(outcomes={"MINE": None, "OTHER": {"state": "OPEN", "ticket": 99}})
    r = _make_runner(ex, j, tag="macd:1h")
    r._recover()
    assert [c for c, _ in ex.outcome_calls] == ["MINE"]   # 只查本 tag
    assert {row.client_order_id for row in j.pending()} == {"OTHER"}


def test_recover_filters_by_account():
    """pending 按 account 过滤：他账户意图不收敛"""
    j = InMemoryJournal()
    j.begin(client_order_id="A1", account="acct", tag="macd:1h", symbol="EURUSD",
            side="BUY", offset="OPEN", qty=Decimal("0.1"))
    j.begin(client_order_id="B1", account="other", tag="macd:1h", symbol="EURUSD",
            side="BUY", offset="OPEN", qty=Decimal("0.1"))
    ex = _RecoverExec(outcomes={"A1": None, "B1": {"state": "OPEN", "ticket": 1}})
    r = _make_runner(ex, j, account="acct")
    r._recover()
    assert [c for c, _ in ex.outcome_calls] == ["A1"]
    assert {row.client_order_id for row in j.pending()} == {"B1"}


# ─── reconcile：magic 隔离 + 缺口2 挂单 in_flight 恢复 ───

def test_reconcile_passes_magic_to_queries():
    """reconcile 把 magic 透传给 query_positions/query_orders（隔离本策略）"""
    ledger = ExposureLedger()
    seen = {}

    class _Exec:
        def query_positions(self, symbol="", magic=None):
            seen["pos_magic"] = magic
            return ([{"type": 0, "volume": 0.3, "price_open": 1.1, "magic": 111}]
                    if magic == 111 else [])

        def query_orders(self, symbol="", magic=None):
            seen["ord_magic"] = magic
            return []

    _reconcile_net_positions(_Exec(), ledger, ["EURUSD"], "macd:1h", venue="MT5", magic=111)
    assert seen == {"pos_magic": 111, "ord_magic": 111}
    assert ledger.net_position("EURUSD") == Decimal("0.3")


def test_reconcile_gap2_restores_orphan_in_flight():
    """缺口2：venue 孤儿挂单（journal 无）凭 client_order_id 恢复 in_flight"""
    ledger = ExposureLedger()

    class _Exec:
        def query_positions(self, symbol="", magic=None):
            return []

        def query_orders(self, symbol="", magic=None):
            return [{"ticket": 77, "symbol": "EURUSD", "type": 0,
                     "volume_current": 0.2, "client_order_id": "ORPHAN"}]

    _reconcile_net_positions(_Exec(), ledger, ["EURUSD"], "macd:1h", magic=None)
    pos = ledger.position("EURUSD", "macd:1h")
    assert pos.in_flight == Decimal("0.2")                # type0=BUY OPEN → +0.2
    assert ledger.is_order_tracked("EURUSD", "macd:1h", "ORPHAN")


def test_reconcile_gap2_skips_already_tracked_no_double_count():
    """_recover 已跟踪的挂单，reconcile 跳过（in_flight 不双计）"""
    ledger = ExposureLedger()
    ledger.on_order_accepted("EURUSD", "macd:1h", "ORPHAN",
                             OrderSide.BUY, Offset.OPEN, Decimal("0.2"))

    class _Exec:
        def query_positions(self, symbol="", magic=None):
            return []

        def query_orders(self, symbol="", magic=None):
            return [{"ticket": 77, "symbol": "EURUSD", "type": 0,
                     "volume_current": 0.2, "client_order_id": "ORPHAN"}]

    _reconcile_net_positions(_Exec(), ledger, ["EURUSD"], "macd:1h", magic=None)
    assert ledger.position("EURUSD", "macd:1h").in_flight == Decimal("0.2")  # 非 0.4


def test_reconcile_gap2_uses_comment_and_volume_initial():
    """MT5 挂单：无 client_order_id 时用 comment 作 coid，量取 volume_initial"""
    ledger = ExposureLedger()

    class _Exec:
        def query_positions(self, symbol="", magic=None):
            return []

        def query_orders(self, symbol="", magic=None):
            return [{"ticket": 88, "symbol": "EURUSD", "type": 1,
                     "volume_initial": 0.15, "comment": "KQ-x"}]

    _reconcile_net_positions(_Exec(), ledger, ["EURUSD"], "macd:1h", magic=None)
    pos = ledger.position("EURUSD", "macd:1h")
    assert pos.in_flight == Decimal("-0.15")              # type1=SELL → -0.15
    assert ledger.is_order_tracked("EURUSD", "macd:1h", "KQ-x")


# ─── Mt5Executor.query_order_outcome + magic 过滤 ───

class _MockDriver:
    def __init__(self, positions=None, orders=None, deals=None, account=None):
        self._positions = positions or []
        self._orders = orders or []
        self._deals = deals or []
        self._account = account

    @staticmethod
    def _by_symbol(rows, symbol):
        return [r for r in rows if not symbol or r.get("symbol") == symbol]

    def positions_get(self, symbol=""):
        return self._by_symbol(self._positions, symbol)

    def orders_get(self, symbol=""):
        return self._by_symbol(self._orders, symbol)

    def history_deals_get(self, position=0):
        return self._deals

    def account_info(self):
        return self._account


def test_mt5_query_positions_filters_magic():
    drv = _MockDriver(positions=[
        {"symbol": "EURUSD", "magic": 111, "volume": 0.1, "type": 0, "price_open": 1.1},
        {"symbol": "EURUSD", "magic": 222, "volume": 0.2, "type": 0, "price_open": 1.2},
    ])
    ex = Mt5Executor(drv, magic=202609)
    assert len(ex.query_positions("EURUSD", magic=111)) == 1
    assert len(ex.query_positions("EURUSD")) == 2          # None → 不过滤（向后兼容）


def test_mt5_query_orders_filters_magic():
    drv = _MockDriver(orders=[
        {"symbol": "EURUSD", "magic": 111, "ticket": 1, "comment": "a"},
        {"symbol": "EURUSD", "magic": 222, "ticket": 2, "comment": "b"},
    ])
    ex = Mt5Executor(drv, magic=202609)
    assert len(ex.query_orders("EURUSD", magic=222)) == 1
    assert ex.query_orders("EURUSD", magic=222)[0]["ticket"] == 2


def test_mt5_query_order_outcome_open():
    drv = _MockDriver(orders=[{"symbol": "EURUSD", "comment": "K1", "ticket": 5}])
    assert Mt5Executor(drv).query_order_outcome("K1", "EURUSD") == {
        "state": "OPEN", "ticket": 5,
    }


def test_mt5_query_order_outcome_filled_by_position():
    drv = _MockDriver(positions=[
        {"symbol": "EURUSD", "comment": "K2", "ticket": 7, "volume": 0.1, "price_open": 1.1},
    ])
    out = Mt5Executor(drv).query_order_outcome("K2", "EURUSD")
    assert out["state"] == "FILLED" and out["ticket"] == 7
    assert out["filled_qty"] == Decimal("0.1")
    assert out["filled_price"] == Decimal("1.1")


def test_mt5_query_order_outcome_filled_by_deals():
    drv = _MockDriver(deals=[{"comment": "K3", "order": 9, "volume": 0.1, "price": 1.2}])
    out = Mt5Executor(drv).query_order_outcome("K3", "EURUSD")
    assert out["state"] == "FILLED" and out["ticket"] == 9
    assert out["filled_qty"] == Decimal("0.1")
    assert out["filled_price"] == Decimal("1.2")


def test_mt5_query_order_outcome_none_when_absent():
    assert Mt5Executor(_MockDriver()).query_order_outcome("NOPE", "EURUSD") is None
