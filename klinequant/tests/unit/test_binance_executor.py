"""BinanceExecutor 单元测试

覆盖：
  - submit 同步桥接（async REST → run_coroutine_threadsafe → SubmitResult）
  - MARKET OPEN → FILLED；CLOSE → reduceOnly=true；LIMIT → IN_FLIGHT；拒单 → DEAD
  - query_positions / query_account / query_orders 返回 MT5 同形 dict
用真实 event loop 线程 + AsyncMock httpx client 验证桥接与形状转换。
"""
import asyncio
import threading
from decimal import Decimal
from unittest import mock

import pytest

from core.trade_engine.executors.binance_executor import BinanceExecutor
from core.trade_engine.resolver import VenueOrderSpec
from protocol.types import DeadReason, Offset, OrderKind, OrderSide


class _Resp:
    """假 httpx.Response"""

    def __init__(self, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json = json_data if json_data is not None else {}
        self.text = text
        self.headers = {"content-type": "application/json"}

    def json(self):
        return self._json


@pytest.fixture
def loop():
    lp = asyncio.new_event_loop()
    t = threading.Thread(target=lp.run_forever, daemon=True)
    t.start()
    yield lp
    lp.call_soon_threadsafe(lp.stop)
    t.join(timeout=5)
    lp.close()


def _executor(loop, client):
    return BinanceExecutor(
        loop=loop, client=client, api_key="k", api_secret="s", magic=202609,
    )


def _spec(side, offset, kind=OrderKind.MARKET, qty="0.001", price=None):
    return VenueOrderSpec(
        symbol="BTCUSDT", side=side, offset=offset, qty=Decimal(qty),
        kind=kind, price=Decimal(price) if price else None,
        client_order_id="kq-test",
    )


# ─── submit ───

def test_submit_market_open_filled(loop):
    client = mock.Mock()
    client.post = mock.AsyncMock(return_value=_Resp(200, {
        "orderId": 123, "status": "FILLED",
        "executedQty": "0.001", "avgPrice": "50000.5",
    }))
    ex = _executor(loop, client)

    res = ex.submit(_spec(OrderSide.BUY, Offset.OPEN))

    assert res.status == "FILLED"
    assert res.success is True
    assert res.order_ticket == 123
    assert res.filled_qty == Decimal("0.001")
    assert res.filled_price == Decimal("50000.5")
    # 验证请求参数：MARKET / BUY / BOTH / 无 reduceOnly
    _, kwargs = client.post.call_args
    params = kwargs["params"]
    assert params["type"] == "MARKET"
    assert params["side"] == "BUY"
    assert params["positionSide"] == "BOTH"
    assert params["symbol"] == "BTCUSDT"
    assert "reduceOnly" not in params
    assert "signature" in params


def test_submit_market_close_sets_reduce_only(loop):
    client = mock.Mock()
    client.post = mock.AsyncMock(return_value=_Resp(200, {
        "orderId": 7, "status": "FILLED", "executedQty": "0.001", "avgPrice": "50000",
    }))
    ex = _executor(loop, client)

    res = ex.submit(_spec(OrderSide.SELL, Offset.CLOSE))

    assert res.status == "FILLED"
    _, kwargs = client.post.call_args
    assert kwargs["params"]["reduceOnly"] == "true"
    assert kwargs["params"]["side"] == "SELL"


def test_submit_limit_in_flight(loop):
    client = mock.Mock()
    client.post = mock.AsyncMock(return_value=_Resp(200, {
        "orderId": 55, "status": "NEW", "executedQty": "0", "avgPrice": "0",
    }))
    ex = _executor(loop, client)

    res = ex.submit(_spec(OrderSide.BUY, Offset.OPEN, kind=OrderKind.LIMIT,
                          price="49000"))

    assert res.status == "IN_FLIGHT"
    assert res.success is True
    assert res.order_ticket == 55
    _, kwargs = client.post.call_args
    params = kwargs["params"]
    assert params["type"] == "LIMIT"
    assert params["price"] == "49000"
    assert params["timeInForce"] == "GTC"


def test_submit_rejected_dead(loop):
    client = mock.Mock()
    client.post = mock.AsyncMock(return_value=_Resp(400, {
        "code": -1111, "msg": "Precision is over the maximum defined",
    }))
    ex = _executor(loop, client)

    res = ex.submit(_spec(OrderSide.BUY, Offset.OPEN))

    assert res.status == "DEAD"
    assert res.success is False
    assert res.dead_reason == DeadReason.REJECTED.value
    assert "-1111" in res.comment


def test_submit_unsupported_kind_dead(loop):
    client = mock.Mock()
    client.post = mock.AsyncMock(return_value=_Resp(200, {}))
    ex = _executor(loop, client)

    res = ex.submit(_spec(OrderSide.BUY, Offset.OPEN, kind=OrderKind.STOP_MARKET))

    assert res.status == "DEAD"
    client.post.assert_not_called()


# ─── cancel ───

def test_cancel_ok(loop):
    client = mock.Mock()
    client.delete = mock.AsyncMock(return_value=_Resp(200, {"orderId": 9, "status": "CANCELED"}))
    ex = _executor(loop, client)
    assert ex.cancel(9, "BTCUSDT") is True
    _, kwargs = client.delete.call_args
    assert kwargs["params"]["orderId"] == 9
    assert kwargs["params"]["symbol"] == "BTCUSDT"


def test_cancel_fail(loop):
    client = mock.Mock()
    client.delete = mock.AsyncMock(return_value=_Resp(400, {"code": -2011, "msg": "Unknown order"}))
    ex = _executor(loop, client)
    assert ex.cancel(9, "BTCUSDT") is False


# ─── query_positions（MT5 同形）───

def test_query_positions_long_short_shapes(loop):
    client = mock.Mock()
    client.get = mock.AsyncMock(return_value=_Resp(200, [
        {"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "50000",
         "markPrice": "51000", "unRealizedProfit": "500", "updateTime": 1700000000000},
        {"symbol": "ETHUSDT", "positionAmt": "-2.0", "entryPrice": "3000",
         "markPrice": "2900", "unRealizedProfit": "200", "updateTime": 1700000000000},
        {"symbol": "XRPUSDT", "positionAmt": "0", "entryPrice": "0"},  # 空仓跳过
    ]))
    ex = _executor(loop, client)

    positions = ex.query_positions()
    assert len(positions) == 2

    long = next(p for p in positions if p["symbol"] == "BTCUSDT")
    assert long["type"] == 0                 # 多
    assert long["volume"] == 0.5
    assert long["price_open"] == 50000.0
    assert long["price_current"] == 51000.0
    assert long["profit"] == 500.0
    assert long["magic"] == 202609
    assert isinstance(long["ticket"], int)

    short = next(p for p in positions if p["symbol"] == "ETHUSDT")
    assert short["type"] == 1                # 空
    assert short["volume"] == 2.0            # 绝对值


def test_query_positions_filters_symbol(loop):
    client = mock.Mock()
    client.get = mock.AsyncMock(return_value=_Resp(200, [
        {"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "50000",
         "markPrice": "50000", "unRealizedProfit": "0", "updateTime": 0},
    ]))
    ex = _executor(loop, client)
    assert len(ex.query_positions("BTCUSDT")) == 1
    assert ex.query_positions("ETHUSDT") == []


# ─── query_account（MT5 同形）───

def test_query_account_usdt_shape(loop):
    client = mock.Mock()
    client.get = mock.AsyncMock(return_value=_Resp(200, [
        {"asset": "BNB", "balance": "1", "availableBalance": "1", "crossUnPnl": "0"},
        {"asset": "USDT", "balance": "5000", "availableBalance": "4500", "crossUnPnl": "100"},
    ]))
    ex = _executor(loop, client)

    acct = ex.query_account()
    assert acct["balance"] == 5000.0
    assert acct["margin_free"] == 4500.0
    assert acct["margin"] == 500.0            # balance - available
    assert acct["profit"] == 100.0
    assert acct["equity"] == 5100.0           # balance + upnl
    assert acct["currency"] == "USDT"


def test_query_account_error_returns_none(loop):
    client = mock.Mock()
    client.get = mock.AsyncMock(return_value=_Resp(401, {"code": -2015, "msg": "Invalid API-key"}))
    ex = _executor(loop, client)
    assert ex.query_account() is None


# ─── query_orders（MT5 同形）───

def test_query_orders_shape(loop):
    client = mock.Mock()
    client.get = mock.AsyncMock(return_value=_Resp(200, [
        {"orderId": 42, "symbol": "BTCUSDT", "side": "BUY", "origQty": "0.5",
         "price": "49000", "time": 1700000000000},
        {"orderId": 43, "symbol": "BTCUSDT", "side": "SELL", "origQty": "0.2",
         "price": "52000", "time": 1700000001000},
    ]))
    ex = _executor(loop, client)

    orders = ex.query_orders("BTCUSDT")
    assert len(orders) == 2
    buy = orders[0]
    assert buy["ticket"] == 42
    assert buy["type"] == 0
    assert buy["volume_current"] == 0.5
    assert buy["price_open"] == 49000.0
    assert buy["time_setup"] == 1700000000
    assert orders[1]["type"] == 1
