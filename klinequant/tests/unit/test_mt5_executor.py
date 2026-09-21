"""Mt5Executor 平仓单元测试（Hedging 安全 + Netting 兼容）

覆盖 2026-09-21 实盘事故根因：对冲账户下"反向市价单"会误开新仓，
修复后平仓按 position ticket 精确平指定持仓，无匹配持仓则拒单不发裸单。
"""
from decimal import Decimal

from core.trade_engine.executors.mt5_executor import Mt5Executor
from core.trade_engine.resolver import VenueOrderSpec
from protocol.types import Offset, OrderKind, OrderSide


class _FakeDriver:
    """记录 order_send 请求的假驱动"""

    def __init__(self, positions=None, tick=None, send_result=None):
        self._positions = positions or []
        self._tick = tick or {"bid": 1.1478, "ask": 1.1479}
        # send_result: dict（固定返回）或 callable(request)->dict
        self._send_result = send_result if send_result is not None else {
            "retcode": 10009, "order": 111, "deal": 222,
            "volume": 0.0, "price": 0.0, "comment": "Request executed",
        }
        self.sent = []   # 收到的 order_send 请求列表

    def positions_get(self, symbol=""):
        return [p for p in self._positions if not symbol or p.get("symbol") == symbol]

    def symbol_info_tick(self, symbol):
        return dict(self._tick)

    def order_send(self, request):
        self.sent.append(request)
        res = self._send_result(request) if callable(self._send_result) else self._send_result
        # 回填成交量/价，模拟真实 DEAL 成交
        out = dict(res)
        if out.get("retcode") == 10009:
            out.setdefault("volume", request["volume"])
            out["volume"] = request["volume"]
            out["price"] = request["price"]
        return out


def _close_spec(side, qty, symbol="EURUSD", close_ticket=0):
    return VenueOrderSpec(
        symbol=symbol, side=side, offset=Offset.CLOSE, qty=Decimal(str(qty)),
        kind=OrderKind.MARKET, close_ticket=close_ticket,
        client_order_id="KQ-test",
    )


def test_close_short_targets_position_ticket():
    """平空(BUY CLOSE)：选中 type=1 空单，请求带 position=ticket，成交量=持仓量"""
    drv = _FakeDriver(positions=[
        {"ticket": 9001, "symbol": "EURUSD", "type": 1, "volume": 0.01, "time": 100},
    ])
    ex = Mt5Executor(drv)
    res = ex.submit(_close_spec(OrderSide.BUY, 0.01))
    assert res.status == "FILLED"
    assert res.filled_qty == Decimal("0.01")
    assert len(drv.sent) == 1
    assert drv.sent[0]["position"] == 9001          # 关键：带 ticket
    assert drv.sent[0]["type"] == 0                 # BUY
    assert drv.sent[0]["action"] == 1               # DEAL


def test_close_with_no_matching_position_rejects_not_naked_order():
    """无匹配持仓（持多却发 BUY 平空）→ DEAD 且绝不发 order_send（防误开新仓）"""
    drv = _FakeDriver(positions=[
        {"ticket": 9001, "symbol": "EURUSD", "type": 0, "volume": 0.01, "time": 100},  # 多单
    ])
    ex = Mt5Executor(drv)
    res = ex.submit(_close_spec(OrderSide.BUY, 0.01))   # BUY 平空，但只有多单
    assert res.status == "DEAD"
    assert res.success is False
    assert drv.sent == []                                # 未发任何裸单


def test_close_no_position_at_all_rejects():
    """完全无持仓 → DEAD，不发单"""
    drv = _FakeDriver(positions=[])
    ex = Mt5Executor(drv)
    res = ex.submit(_close_spec(OrderSide.SELL, 0.01))
    assert res.status == "DEAD"
    assert drv.sent == []


def test_hedging_fifo_close_spans_multiple_positions():
    """对冲账户多笔同向持仓：FIFO 逐笔平，两笔各带自己的 ticket"""
    drv = _FakeDriver(positions=[
        {"ticket": 9002, "symbol": "EURUSD", "type": 0, "volume": 0.01, "time": 200},  # 较晚
        {"ticket": 9001, "symbol": "EURUSD", "type": 0, "volume": 0.01, "time": 100},  # 较早
    ])
    ex = Mt5Executor(drv)
    res = ex.submit(_close_spec(OrderSide.SELL, 0.02))   # 平掉全部 0.02 多单
    assert res.status == "FILLED"
    assert res.filled_qty == Decimal("0.02")
    assert len(drv.sent) == 2
    assert drv.sent[0]["position"] == 9001   # FIFO：先平较早的
    assert drv.sent[1]["position"] == 9002


def test_close_ignores_opposite_direction_positions():
    """同时有多空持仓时，平多只选 type=0，不碰空单"""
    drv = _FakeDriver(positions=[
        {"ticket": 9001, "symbol": "EURUSD", "type": 1, "volume": 0.01, "time": 100},  # 空
        {"ticket": 9002, "symbol": "EURUSD", "type": 0, "volume": 0.01, "time": 200},  # 多
    ])
    ex = Mt5Executor(drv)
    res = ex.submit(_close_spec(OrderSide.SELL, 0.01))   # SELL 平多
    assert res.status == "FILLED"
    assert len(drv.sent) == 1
    assert drv.sent[0]["position"] == 9002   # 只平多单


def test_explicit_close_ticket_bypasses_query():
    """显式指定 close_ticket 时直接按该 ticket 平，不查持仓"""
    drv = _FakeDriver(positions=[])   # 空持仓，但显式给了 ticket
    ex = Mt5Executor(drv)
    res = ex.submit(_close_spec(OrderSide.BUY, 0.01, close_ticket=7777))
    assert res.status == "FILLED"
    assert len(drv.sent) == 1
    assert drv.sent[0]["position"] == 7777


def test_open_market_unaffected():
    """开仓路径不受影响：正常 DEAL，不带 position 字段"""
    drv = _FakeDriver()
    ex = Mt5Executor(drv)
    spec = VenueOrderSpec(
        symbol="EURUSD", side=OrderSide.BUY, offset=Offset.OPEN,
        qty=Decimal("0.01"), kind=OrderKind.MARKET, client_order_id="KQ-open",
    )
    res = ex.submit(spec)
    assert res.status == "FILLED"
    assert len(drv.sent) == 1
    assert "position" not in drv.sent[0]
    assert drv.sent[0]["type"] == 0   # BUY
