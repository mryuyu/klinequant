"""Phase R 崩溃恢复矩阵（kill-process matrix）集成单测。

以「真实持久化 WAL（SqliteJournal, synchronous=FULL）+ 进程重启」重演 send_order
生命周期各杀进程点，验证 R1~R4 协同收敛（《SDK 阶段实施规划 v1.3》Phase R 验收）：

  P0 begin 前死    → WAL 空           → clean start，不探测 venue
  P1 begin 后死    → SUBMITTING/查无  → DEAD 释放，账本无幻影持仓
  P2a submit 后死  → SUBMITTING/挂单  → in_flight+ticket，WAL→IN_FLIGHT
  P2b submit 后死  → SUBMITTING/成交  → 补账本持仓，WAL→FILLED
  P2c submit 抛死  → UNKNOWN/成交     → 补账本持仓，WAL→FILLED
  P3 finish 后死   → FILLED(终态)     → 无 pending（交 reconcile），recover 不双计
  P4 挂单在途重启  → IN_FLIGHT        → 幂等重收敛，in_flight 不累积

关键：WAL 经 fsync 落盘，硬杀（不 close）后重开仍在（先写后发不丢已 begin 意图）。
"""
import sqlite3
from decimal import Decimal

from core.trade_engine.executors.mt5_executor import SubmitResult
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from protocol.types import Offset, OrderKind, OrderSide, SymbolInfo
from strategy.sdk.api import KqApi
from strategy.sdk.live_runner import LiveRunner
from strategy.sdk.order_journal import (
    STATE_DEAD,
    STATE_FILLED,
    STATE_IN_FLIGHT,
    STATE_SUBMITTING,
    STATE_UNKNOWN,
    SqliteJournal,
)

# ─── 常量 ───

TAG = "macd:1h"
ACCT = "acct"

# ─── 脚手架 ───


def _fx_spec(symbol="EURUSD"):
    return SymbolInfo(
        symbol=symbol, market_type="FX",
        pip_size=Decimal("0.0001"), tick_size=Decimal("0.00001"),
        qty_step=Decimal("0.01"), min_qty=Decimal("0.01"), qty_max=Decimal("200"),
        can_short=True,
    )


class _FakeBackend:
    """_recover 不触发 backend；仅供 LiveRunner 构造。"""


class _StubApi:
    """_recover 只写 api._order_tickets，用桩替代 KqApi。"""

    def __init__(self, symbol):
        self._symbol = symbol
        self._order_tickets = {}


class _VenueExec:
    """恢复期 venue 桩：query_account 探活 + query_order_outcome 反映杀点 venue 真相。"""

    def __init__(self, outcomes=None):
        self._outcomes = outcomes or {}
        self.probe_calls = 0

    def query_account(self):
        self.probe_calls += 1
        return {"balance": 1.0}

    def query_order_outcome(self, client_order_id, symbol=""):
        return self._outcomes.get(client_order_id)


class _SubmitExec:
    """send_order 用执行器桩：submit 返回预设 SubmitResult，记录 spec（取 coid）。"""

    def __init__(self, status="IN_FLIGHT", ticket=1):
        self._status = status
        self._ticket = ticket
        self.submitted = []

    def submit(self, spec):
        self.submitted.append(spec)
        filled = self._status == "FILLED"
        return SubmitResult(
            success=True, status=self._status, order_ticket=self._ticket,
            filled_qty=Decimal("0.10") if filled else Decimal("0"),
            filled_price=Decimal("1.0900") if filled else None,
            comment="ok" if filled else "placed",
        )

    def cancel(self, order_ticket, symbol, magic=None):
        return True

    def query_positions(self, symbol="", magic=None):
        return []

    def query_account(self):
        return None

    def query_orders(self, symbol="", magic=None):
        return []


class _NullFeed:
    def now_ms(self):
        return 0

    def latest_tick(self, symbol):
        return None

    def latest_bars(self, symbol, period, count):
        return []

    def wait_update(self, deadline=None):
        return False

    def is_changing(self, obj, field=None):
        return False


def _disk_status(journal_path, coid):
    """直接读盘上 WAL 的 status（独立连接，证明落盘真相；在 journal.close 后调用）。"""
    conn = sqlite3.connect(str(journal_path))
    try:
        row = conn.execute(
            "SELECT status FROM order_journal WHERE client_order_id=?", (coid,)
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _crash_after_begin(journal_path, coid, *, side="BUY", offset="OPEN", qty="0.10",
                       symbol="EURUSD", finish_status=None):
    """重演杀点：begin 落盘（fsync）后进程死；finish_status 非 None 时先落该态再死。

    硬杀模拟：写完直接丢引用、**不 close**（synchronous=FULL 已 fsync，重开仍在）。
    """
    j = SqliteJournal(journal_path)
    j.begin(client_order_id=coid, account=ACCT, tag=TAG, symbol=symbol,
            side=side, offset=offset, qty=Decimal(qty))
    if finish_status is not None:
        j.finish(coid, finish_status, ticket=5,
                 filled_qty=Decimal(qty), filled_price=Decimal("1.1000"))
    del j      # 不 close：模拟 kill -9 / 断电（靠已 fsync 的 WAL 兜底）


def _restart_runner(journal_path, executor, *, symbols=("EURUSD",), tag=TAG, account=ACCT):
    """模拟进程重启：重开持久化 WAL + 全新空账本 + 桩 api，返回待 _recover 的 runner。"""
    journal = SqliteJournal(journal_path)          # 重开同一 WAL 文件
    runner = LiveRunner(
        _FakeBackend(), symbols=list(symbols), period="1h",
        strategy_fn=lambda api: None, tag=tag,
        account_name=account, journal=journal,
        recover_retries=1, recover_backoff=0.0,
    )
    runner._executor = executor
    runner._ledger = ExposureLedger()              # 重启后账本从零（内存态全丢）
    runner._apis = {s: _StubApi(s) for s in symbols}
    return runner, journal


def _make_live_api(journal, executor):
    """端到端用：真实 KqApi（resolver/ledger/specs 齐全）+ 持久化 journal。"""
    return KqApi(
        symbol="EURUSD", period="1h", tag=TAG,
        specs={"EURUSD": _fx_spec()}, ledger=ExposureLedger(),
        resolver=UnifiedResolver(), executor=executor, feed=_NullFeed(),
        account_name=ACCT, journal=journal,
    )


# ═══════════════════════════════════════════
# 杀进程矩阵：各杀点 → 重启 → 收敛
# ═══════════════════════════════════════════

def test_matrix_p0_clean_start_no_record(tmp_path):
    """P0 begin 前崩溃：WAL 空 → 重启 clean start，不探测 venue，账本干净"""
    path = tmp_path / "j.db"                       # 进程在 begin 前就死了，无任何意图
    ex = _VenueExec()
    runner, journal = _restart_runner(path, ex)
    runner._recover()
    assert ex.probe_calls == 0                     # 无 pending → 不探测
    assert journal.pending() == []
    pos = runner._ledger.position("EURUSD", TAG)
    assert pos.volume == Decimal("0") and pos.in_flight == Decimal("0")
    journal.close()


def test_matrix_p1_killed_after_begin_venue_missing_marks_dead(tmp_path):
    """P1 begin 落盘后、submit 到达 venue 前崩溃：查无此单 → DEAD 释放，无幻影持仓"""
    path = tmp_path / "j.db"
    _crash_after_begin(path, "KQ-P1")
    ex = _VenueExec(outcomes={"KQ-P1": None})      # venue 从未收到该单
    runner, journal = _restart_runner(path, ex)
    runner._recover()
    assert journal.pending() == []                 # 已收敛离开 pending
    journal.close()
    assert _disk_status(path, "KQ-P1") == STATE_DEAD
    pos = runner._ledger.position("EURUSD", TAG)
    assert pos.volume == Decimal("0") and pos.in_flight == Decimal("0")


def test_matrix_p2a_killed_after_submit_venue_open_restores_inflight(tmp_path):
    """P2a submit 后、finish 前崩溃，venue 挂单仍在：恢复 in_flight + ticket"""
    path = tmp_path / "j.db"
    _crash_after_begin(path, "KQ-P2A")
    ex = _VenueExec(outcomes={"KQ-P2A": {"state": "OPEN", "ticket": 22}})
    runner, journal = _restart_runner(path, ex)
    runner._recover()
    pend = journal.pending()
    assert len(pend) == 1 and pend[0].status == STATE_IN_FLIGHT
    assert pend[0].ticket == 22
    pos = runner._ledger.position("EURUSD", TAG)
    assert pos.in_flight == Decimal("0.10") and pos.volume == Decimal("0")
    assert runner._apis["EURUSD"]._order_tickets["KQ-P2A"] == 22
    journal.close()


def test_matrix_p2b_killed_after_submit_venue_filled_patches_ledger(tmp_path):
    """P2b submit 后、finish 前崩溃，venue 已成交：补账本持仓 + WAL→FILLED"""
    path = tmp_path / "j.db"
    _crash_after_begin(path, "KQ-P2B", qty="0.10")
    ex = _VenueExec(outcomes={"KQ-P2B": {
        "state": "FILLED", "ticket": 33,
        "filled_qty": Decimal("0.10"), "filled_price": Decimal("1.1000"),
    }})
    runner, journal = _restart_runner(path, ex)
    runner._recover()
    assert journal.pending() == []
    journal.close()
    assert _disk_status(path, "KQ-P2B") == STATE_FILLED
    pos = runner._ledger.position("EURUSD", TAG)
    assert pos.volume == Decimal("0.10") and pos.in_flight == Decimal("0")
    assert runner._apis["EURUSD"]._order_tickets["KQ-P2B"] == 33


def test_matrix_p2c_unknown_state_converges_via_outcome(tmp_path):
    """P2c submit 抛异常落 UNKNOWN 后崩溃：重启凭 venue 反查收敛为 FILLED"""
    path = tmp_path / "j.db"
    _crash_after_begin(path, "KQ-P2C", finish_status=STATE_UNKNOWN)
    ex = _VenueExec(outcomes={"KQ-P2C": {
        "state": "FILLED", "ticket": 44,
        "filled_qty": Decimal("0.10"), "filled_price": Decimal("1.1000"),
    }})
    runner, journal = _restart_runner(path, ex)
    runner._recover()
    assert journal.pending() == []
    journal.close()
    assert _disk_status(path, "KQ-P2C") == STATE_FILLED
    assert runner._ledger.position("EURUSD", TAG).volume == Decimal("0.10")


def test_matrix_p3_terminal_filled_not_repended_no_double_count(tmp_path):
    """P3 成交落 FILLED 终态后崩溃：无 pending → recover 不探测、不重复补账本（防双计）"""
    path = tmp_path / "j.db"
    _crash_after_begin(path, "KQ-P3", finish_status=STATE_FILLED)
    ex = _VenueExec()                              # 持仓由 reconcile 从 venue 恢复
    runner, journal = _restart_runner(path, ex)
    runner._recover()
    assert ex.probe_calls == 0                     # 终态无 pending → 不探测
    assert journal.pending() == []
    assert runner._ledger.position("EURUSD", TAG).volume == Decimal("0")
    journal.close()
    assert _disk_status(path, "KQ-P3") == STATE_FILLED


def test_matrix_p4_inflight_idempotent_across_restarts(tmp_path):
    """P4 挂单在途时崩溃，连续两次重启：每次凭 venue OPEN 幂等重收敛，in_flight 不累积"""
    path = tmp_path / "j.db"
    _crash_after_begin(path, "KQ-P4", finish_status=STATE_IN_FLIGHT)
    outcomes = {"KQ-P4": {"state": "OPEN", "ticket": 42}}
    r1, j1 = _restart_runner(path, _VenueExec(outcomes=outcomes))
    r1._recover()
    assert r1._ledger.position("EURUSD", TAG).in_flight == Decimal("0.10")
    j1.close()
    # 二次重启：全新空账本，WAL 仍 IN_FLIGHT pending → 幂等重收敛
    r2, j2 = _restart_runner(path, _VenueExec(outcomes=outcomes))
    r2._recover()
    assert r2._ledger.position("EURUSD", TAG).in_flight == Decimal("0.10")   # 非 0.20
    assert r2._apis["EURUSD"]._order_tickets["KQ-P4"] == 42
    j2.close()


# ═══════════════════════════════════════════
# WAL 持久性 + 端到端（真实 send_order → 崩溃 → 恢复）
# ═══════════════════════════════════════════

def test_matrix_wal_durable_without_clean_close(tmp_path):
    """先写后发 + fsync：begin 后硬杀（不 close），重开 WAL 意图仍在（账本级不丢）"""
    path = tmp_path / "j.db"
    _crash_after_begin(path, "KQ-DUR", qty="0.10")   # 内部不 close，模拟 kill -9
    ex = _VenueExec(outcomes={"KQ-DUR": {
        "state": "FILLED", "ticket": 9,
        "filled_qty": Decimal("0.10"), "filled_price": Decimal("1.1000"),
    }})
    runner, journal = _restart_runner(path, ex)
    pend = journal.pending(account=ACCT, tag=TAG)
    assert len(pend) == 1 and pend[0].client_order_id == "KQ-DUR"
    assert pend[0].status == STATE_SUBMITTING        # begin 落盘态幸存于硬杀
    runner._recover()
    assert runner._ledger.position("EURUSD", TAG).volume == Decimal("0.10")
    journal.close()


def test_matrix_send_order_inflight_end_to_end_crash_recover(tmp_path):
    """端到端：真实 send_order 写 WAL（begin+finish IN_FLIGHT）→ 硬杀 → 重启 recover 恢复在途"""
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    ex_submit = _SubmitExec(status="IN_FLIGHT", ticket=1234)
    api = _make_live_api(journal, ex_submit)
    res = api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"),
                         kind=OrderKind.LIMIT, price=Decimal("1.0900"))
    assert res.ok
    coid = ex_submit.submitted[0].client_order_id    # send_order 生成的结构化 coid
    del api, journal                                 # 硬杀（不 close）
    # 重启：venue 显示该挂单仍在
    runner, j2 = _restart_runner(
        path, _VenueExec(outcomes={coid: {"state": "OPEN", "ticket": 1234}}))
    runner._recover()
    assert runner._ledger.position("EURUSD", TAG).in_flight == Decimal("0.10")
    assert runner._apis["EURUSD"]._order_tickets[coid] == 1234
    j2.close()
