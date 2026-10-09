"""Phase R2 订单意图 WAL 单元测试。

覆盖《SDK 阶段实施规划 v1.3》R2 验收点：
  - SqliteJournal next_seq 单调 + 重启续号（持久化，不归零）
  - begin/finish/pending 生命周期与终态/非终态语义
  - pending 按 account/tag 过滤
  - synchronous=FULL 落盘：close 后重开仍见已提交意图
  - InMemoryJournal 与 Sqlite 接口同构
  - KqApi.send_order 接线：begin 先写后发、finish 落终态、submit 异常 → UNKNOWN
"""
from decimal import Decimal

from core.trade_engine.executors.mt5_executor import SubmitResult
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from protocol.types import Offset, OrderKind, OrderSide, SymbolInfo
from strategy.sdk.api import KqApi
from strategy.sdk.order_id import OrderIdFactory
from strategy.sdk.order_journal import (
    STATE_DEAD,
    STATE_FILLED,
    STATE_IN_FLIGHT,
    STATE_SUBMITTING,
    STATE_UNKNOWN,
    InMemoryJournal,
    SqliteJournal,
)

# ─── SqliteJournal：seq ───

def test_sqlite_next_seq_monotonic(tmp_path):
    j = SqliteJournal(tmp_path / "j.db")
    assert j.next_seq("acct", "macd:1h") == 1
    assert j.next_seq("acct", "macd:1h") == 2
    assert j.next_seq("acct", "macd:15m") == 1   # 不同 tag 独立计数
    assert j.next_seq("other", "macd:1h") == 1   # 不同 account 独立计数
    j.close()


def test_sqlite_next_seq_persists_across_reopen(tmp_path):
    """重启续号：seq 持久化，绝不归零（否则 Phase 3 重复跟单）"""
    db = tmp_path / "j.db"
    j = SqliteJournal(db)
    j.next_seq("acct", "t")
    j.next_seq("acct", "t")
    j.close()
    j2 = SqliteJournal(db)
    assert j2.next_seq("acct", "t") == 3
    j2.close()


# ─── SqliteJournal：begin/finish/pending ───

def test_sqlite_begin_pending_finish_lifecycle(tmp_path):
    j = SqliteJournal(tmp_path / "j.db")
    j.begin(client_order_id="KQ-1", account="acct", tag="macd:1h", symbol="EURUSD",
            side="buy", offset="open", qty=Decimal("0.10"), price=Decimal("1.1000"))
    pend = j.pending()
    assert len(pend) == 1
    assert pend[0].status == STATE_SUBMITTING
    assert pend[0].qty == Decimal("0.10")
    assert pend[0].price == Decimal("1.1000")
    assert pend[0].account == "acct" and pend[0].tag == "macd:1h"
    j.finish("KQ-1", STATE_FILLED, ticket=111, filled_qty=Decimal("0.10"),
             filled_price=Decimal("1.1001"))
    assert j.pending() == []   # FILLED 终态离开 pending
    j.close()


def test_sqlite_in_flight_stays_pending(tmp_path):
    j = SqliteJournal(tmp_path / "j.db")
    j.begin(client_order_id="KQ-2", account="a", tag="t", symbol="EURUSD",
            side="buy", offset="open", qty=Decimal("0.10"))
    j.finish("KQ-2", STATE_IN_FLIGHT, ticket=222)
    pend = j.pending()
    assert len(pend) == 1 and pend[0].status == STATE_IN_FLIGHT and pend[0].ticket == 222
    j.close()


def test_sqlite_dead_leaves_pending(tmp_path):
    j = SqliteJournal(tmp_path / "j.db")
    j.begin(client_order_id="KQ-3", account="a", tag="t", symbol="EURUSD",
            side="buy", offset="open", qty=Decimal("0.10"))
    j.finish("KQ-3", STATE_DEAD, reason="rejected")
    assert j.pending() == []
    j.close()


def test_sqlite_unknown_stays_pending(tmp_path):
    j = SqliteJournal(tmp_path / "j.db")
    j.begin(client_order_id="KQ-4", account="a", tag="t", symbol="EURUSD",
            side="buy", offset="open", qty=Decimal("0.10"))
    j.finish("KQ-4", STATE_UNKNOWN, reason="submit exception")
    pend = j.pending()
    assert len(pend) == 1 and pend[0].status == STATE_UNKNOWN
    j.close()


def test_sqlite_pending_filter_by_account_tag(tmp_path):
    j = SqliteJournal(tmp_path / "j.db")
    for cid, acct, tag in [("A", "acct1", "macd:1h"), ("B", "acct2", "macd:1h"),
                           ("C", "acct1", "macd:15m")]:
        j.begin(client_order_id=cid, account=acct, tag=tag, symbol="EURUSD",
                side="buy", offset="open", qty=Decimal("0.1"))
    assert len(j.pending()) == 3
    assert len(j.pending(account="acct1")) == 2
    assert len(j.pending(account="acct1", tag="macd:1h")) == 1
    assert j.pending(account="acct1", tag="macd:1h")[0].client_order_id == "A"
    j.close()


def test_sqlite_durable_reopen_sees_committed(tmp_path):
    """synchronous=FULL：close 后重开仍见已 begin 的意图（崩溃恢复根基）"""
    db = tmp_path / "j.db"
    j = SqliteJournal(db)
    j.begin(client_order_id="KQ-dur", account="a", tag="t", symbol="EURUSD",
            side="buy", offset="open", qty=Decimal("0.10"))
    j.close()
    j2 = SqliteJournal(db)
    pend = j2.pending()
    assert len(pend) == 1 and pend[0].client_order_id == "KQ-dur"
    j2.close()


def test_sqlite_finish_missing_row_no_crash(tmp_path):
    j = SqliteJournal(tmp_path / "j.db")
    j.finish("NOPE", STATE_FILLED)   # 不存在的行：告警不抛
    j.close()


# ─── InMemoryJournal 同构 ───

def test_inmemory_parity():
    j = InMemoryJournal()
    assert j.next_seq("a", "t") == 1
    assert j.next_seq("a", "t") == 2
    j.begin(client_order_id="K1", account="a", tag="t", symbol="EURUSD",
            side="buy", offset="open", qty=Decimal("0.1"))
    assert len(j.pending()) == 1
    j.finish("K1", STATE_FILLED)
    assert j.pending() == []
    j.begin(client_order_id="K2", account="a", tag="t", symbol="EURUSD",
            side="buy", offset="open", qty=Decimal("0.1"))
    j.finish("K2", STATE_IN_FLIGHT, ticket=9)
    pend = j.pending()
    assert len(pend) == 1 and pend[0].ticket == 9
    j.close()


# ─── seq_provider 驱动 client_order_id ───

def test_journal_seq_provider_drives_order_id(tmp_path):
    """OrderIdFactory 复用 journal.next_seq：id seq 与 WAL seq 同源、重启续号"""
    db = tmp_path / "j.db"
    j = SqliteJournal(db)
    f = OrderIdFactory(seq_provider=j.next_seq)
    oid1 = f.generate("acct", "macd:1h")
    oid2 = f.generate("acct", "macd:1h")
    assert oid1.split("-")[3] == "1"
    assert oid2.split("-")[3] == "2"
    j.close()
    j2 = SqliteJournal(db)
    assert j2.next_seq("acct", "macd:1h") == 3
    j2.close()


# ─── KqApi.send_order 接线 ───

def _fx_spec(symbol="EURUSD") -> SymbolInfo:
    return SymbolInfo(
        symbol=symbol, market_type="FX",
        pip_size=Decimal("0.0001"), tick_size=Decimal("0.00001"),
        qty_step=Decimal("0.01"), min_qty=Decimal("0.01"), qty_max=Decimal("200"),
        can_short=True,
    )


class _ResultExecutor:
    """submit 返回预设 SubmitResult（或抛异常）；记录收到的 spec。"""

    def __init__(self, result=None, exc=None):
        self._result = result
        self._exc = exc
        self.submitted = []

    def submit(self, spec):
        self.submitted.append(spec)
        if self._exc is not None:
            raise self._exc
        return self._result

    def cancel(self, order_ticket, symbol, magic=None):
        return True

    def query_positions(self, symbol=""):
        return []

    def query_account(self):
        return None

    def query_orders(self, symbol=""):
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


def _make_api(executor, journal):
    return KqApi(
        symbol="EURUSD", period="1h", tag="macd:1h",
        specs={"EURUSD": _fx_spec()}, ledger=ExposureLedger(),
        resolver=UnifiedResolver(), executor=executor, feed=_NullFeed(),
        account_name="acct", journal=journal,
    )


def test_send_order_writes_begin_then_finish_filled():
    j = InMemoryJournal()
    ex = _ResultExecutor(result=SubmitResult(
        success=True, status="FILLED", order_ticket=1001,
        filled_qty=Decimal("0.10"), filled_price=Decimal("1.1000"), comment="ok",
    ))
    api = _make_api(ex, j)
    res = api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))
    assert res.ok
    assert j.pending() == []              # FILLED 终态
    assert len(j._rows) == 1              # begin 曾写入
    row = next(iter(j._rows.values()))
    assert row.status == STATE_FILLED
    assert row.ticket == 1001
    assert row.account == "acct" and row.tag == "macd:1h"
    assert row.symbol == "EURUSD"


def test_send_order_in_flight_stays_pending():
    j = InMemoryJournal()
    ex = _ResultExecutor(result=SubmitResult(
        success=True, status="IN_FLIGHT", order_ticket=2002, comment="placed",
    ))
    api = _make_api(ex, j)
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"),
                   kind=OrderKind.LIMIT, price=Decimal("1.0900"))
    pend = j.pending()
    assert len(pend) == 1
    assert pend[0].status == STATE_IN_FLIGHT and pend[0].ticket == 2002


def test_send_order_dead_recorded():
    j = InMemoryJournal()
    ex = _ResultExecutor(result=SubmitResult(
        success=False, status="DEAD", dead_reason="rejected",
        retcode=10006, comment="rejected",
    ))
    api = _make_api(ex, j)
    res = api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))
    assert not res.ok
    assert j.pending() == []
    row = next(iter(j._rows.values()))
    assert row.status == STATE_DEAD and "10006" in row.reason


def test_send_order_submit_exception_writes_unknown():
    """submit 抛异常 → 写 UNKNOWN（绝不静默）+ 保守保持在途（pending 非空）"""
    j = InMemoryJournal()
    ex = _ResultExecutor(exc=RuntimeError("connection lost"))
    api = _make_api(ex, j)
    res = api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))
    assert not res.ok and "submit exception" in res.reason
    pend = j.pending()
    assert len(pend) == 1 and pend[0].status == STATE_UNKNOWN
    assert "connection lost" in pend[0].reason


def test_send_order_begin_before_submit_ordering():
    """WAL 先写后发：submit 被调用的那一刻 journal 已有 SUBMITTING 记录"""
    j = InMemoryJournal()
    seen = {}

    class _Spy(_ResultExecutor):
        def submit(self, spec):
            seen["pending_at_submit"] = [r.status for r in j.pending()]
            return super().submit(spec)

    ex = _Spy(result=SubmitResult(
        success=True, status="FILLED", order_ticket=1,
        filled_qty=Decimal("0.1"), filled_price=Decimal("1.1"),
    ))
    api = _make_api(ex, j)
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"))
    assert seen["pending_at_submit"] == [STATE_SUBMITTING]


def test_send_order_with_sqlite_journal_durable(tmp_path):
    db = tmp_path / "acct.db"
    j = SqliteJournal(db)
    ex = _ResultExecutor(result=SubmitResult(
        success=True, status="IN_FLIGHT", order_ticket=3003, comment="placed"))
    api = _make_api(ex, j)
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.10"),
                   kind=OrderKind.LIMIT, price=Decimal("1.0900"))
    j.close()
    # 重开模拟重启：仍见在途意图（R3 恢复消费）
    j2 = SqliteJournal(db)
    pend = j2.pending()
    assert len(pend) == 1 and pend[0].ticket == 3003
    j2.close()
