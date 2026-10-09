"""Phase M5 灾难止损保险丝（venue 侧）单元测试。

覆盖《SDK 阶段实施规划 v1.3》M5 验收点：
  - VenueOrderSpec / OrderRequest 携带 sl/tp；resolver 仅 OPEN 附带、量化到 tick_size
  - CLOSE leg 不带保险丝（平仓无需灾难止损）
  - MT5 _build_request：开仓请求附带 sl/tp（position 属性，随持仓生存亡）
  - api.send_order(sl=,tp=) 透传到 executor 收到的 spec
  - 币安 _place_fuse_async：多头用 SELL / 空头用 BUY 的 STOP_MARKET(SL) +
    TAKE_PROFIT_MARKET(TP)，均 reduceOnly=true（本地平仓后孤儿单不反向开仓）
"""
import asyncio
from decimal import Decimal

from core.trade_engine.executors.binance_executor import BinanceExecutor
from core.trade_engine.executors.mt5_executor import Mt5Executor
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import (
    OrderRequest,
    UnifiedResolver,
    VenueOrderSpec,
)
from protocol.types import Offset, OrderKind, OrderSide
from strategy.sdk.api import KqApi
from tests.unit.test_multi_period import _FillExec, _fx_spec, _NullFeed

# ─── resolver：sl/tp 传播 ───


def _open_req(**kw) -> OrderRequest:
    base = dict(
        symbol="EURUSD", tag="macd:1h", side=OrderSide.BUY, offset=Offset.OPEN,
        qty=Decimal("0.10"), kind=OrderKind.MARKET,
    )
    base.update(kw)
    return OrderRequest(**base)


def test_resolver_open_attaches_sl_tp():
    """OPEN 携带 sl/tp → leg 附带（量化到 tick_size=0.00001，此处已对齐）。"""
    r = UnifiedResolver()
    req = _open_req(sl=Decimal("1.0500"), tp=Decimal("1.1500"))
    res = r.resolve(req, _fx_spec())
    assert res.ok
    assert res.specs[0].sl == Decimal("1.0500")
    assert res.specs[0].tp == Decimal("1.1500")


def test_resolver_open_without_sl_tp_is_none():
    """未给 sl/tp → leg.sl/tp 为 None（后向兼容，无保险丝）。"""
    r = UnifiedResolver()
    res = r.resolve(_open_req(), _fx_spec())
    assert res.ok
    assert res.specs[0].sl is None
    assert res.specs[0].tp is None


def test_resolver_close_legs_no_fuse():
    """CLOSE leg 不带保险丝（sl/tp 仅 OPEN 附带；平仓无需灾难止损）。"""
    r = UnifiedResolver()
    req = _open_req(
        offset=Offset.CLOSE, side=OrderSide.SELL,
        sl=Decimal("1.0500"), tp=Decimal("1.1500"),
    )
    res = r.resolve(req, _fx_spec(), position_volume=Decimal("0.10"),
                    available_to_close=Decimal("0.10"))
    assert res.ok
    for leg in res.specs:
        assert leg.sl is None
        assert leg.tp is None


# ─── MT5：开仓请求附带 sl/tp（position 属性）───


class _TickDriver:
    """最小 MT5 driver 桩：仅提供市价单所需的 symbol_info_tick。"""

    def symbol_info_tick(self, symbol):
        return {"ask": 1.1000, "bid": 1.0990}


def test_mt5_build_request_attaches_sl_tp():
    """MARKET OPEN + sl/tp → 请求 dict 含 sl/tp（float），随持仓生存亡。"""
    ex = Mt5Executor(_TickDriver(), magic=202609)
    spec = VenueOrderSpec(
        symbol="EURUSD", side=OrderSide.BUY, offset=Offset.OPEN,
        qty=Decimal("0.10"), kind=OrderKind.MARKET,
        sl=Decimal("1.0500"), tp=Decimal("1.1500"),
    )
    req = ex._build_request(spec)
    assert req is not None
    assert req["sl"] == 1.0500
    assert req["tp"] == 1.1500


def test_mt5_build_request_no_fuse_keys_when_unset():
    """未给 sl/tp → 请求 dict 不含 sl/tp 键（零回归，普通开仓不变）。"""
    ex = Mt5Executor(_TickDriver(), magic=202609)
    spec = VenueOrderSpec(
        symbol="EURUSD", side=OrderSide.BUY, offset=Offset.OPEN,
        qty=Decimal("0.10"), kind=OrderKind.MARKET,
    )
    req = ex._build_request(spec)
    assert req is not None
    assert "sl" not in req
    assert "tp" not in req


# ─── api.send_order：sl/tp 透传到 spec ───


def test_send_order_passes_sl_tp_to_spec():
    """api.send_order(sl=,tp=) → executor 收到的 spec 携带 sl/tp。"""
    ex = _FillExec()
    api = KqApi(
        symbol="EURUSD", period="1h", tag="macd",
        specs={"EURUSD": _fx_spec()}, ledger=ExposureLedger(),
        resolver=UnifiedResolver(), executor=ex, feed=_NullFeed(),
        account_name="acct",
    )
    api.send_order(
        OrderSide.BUY, Offset.OPEN, Decimal("0.10"),
        sl=Decimal("1.0500"), tp=Decimal("1.1500"),
    )
    assert ex.submitted[0].sl == Decimal("1.0500")
    assert ex.submitted[0].tp == Decimal("1.1500")


# ─── 币安：STOP_MARKET / TAKE_PROFIT_MARKET + reduceOnly 保险丝 ───


class _FakeResp:
    def __init__(self, status=200):
        self.status_code = status
        self.headers = {"content-type": "application/json"}
        self.text = "{}"

    def json(self):
        return {"orderId": 1, "status": "NEW"}


class _FakeClient:
    """记录 post 调用的 async httpx 桩。"""

    def __init__(self):
        self.posts = []

    async def post(self, path, params=None, headers=None):
        self.posts.append(params)
        return _FakeResp(200)


def _binance_ex():
    client = _FakeClient()
    ex = BinanceExecutor(loop=None, client=client, api_key="k", api_secret="s")
    return ex, client


def test_binance_fuse_long_uses_sell_reduce_only():
    """多头开仓 → SL=STOP_MARKET/TP=TAKE_PROFIT_MARKET 均 SELL + reduceOnly。"""
    ex, client = _binance_ex()
    spec = VenueOrderSpec(
        symbol="BTCUSDT", side=OrderSide.BUY, offset=Offset.OPEN,
        qty=Decimal("0.10"), kind=OrderKind.MARKET,
        sl=Decimal("50000"), tp=Decimal("70000"),
    )
    asyncio.run(ex._place_fuse_async(spec))
    assert len(client.posts) == 2
    sl_order, tp_order = client.posts
    assert sl_order["type"] == "STOP_MARKET"
    assert sl_order["side"] == "SELL"
    assert sl_order["reduceOnly"] == "true"
    assert sl_order["stopPrice"] == "50000"
    assert tp_order["type"] == "TAKE_PROFIT_MARKET"
    assert tp_order["side"] == "SELL"
    assert tp_order["reduceOnly"] == "true"
    assert tp_order["stopPrice"] == "70000"


def test_binance_fuse_short_uses_buy():
    """空头开仓 → 保险丝用 BUY 平（方向与开仓相反）。"""
    ex, client = _binance_ex()
    spec = VenueOrderSpec(
        symbol="BTCUSDT", side=OrderSide.SELL, offset=Offset.OPEN,
        qty=Decimal("0.10"), kind=OrderKind.MARKET,
        sl=Decimal("70000"),
    )
    asyncio.run(ex._place_fuse_async(spec))
    assert len(client.posts) == 1               # 仅 sl（未给 tp）
    assert client.posts[0]["type"] == "STOP_MARKET"
    assert client.posts[0]["side"] == "BUY"


def test_binance_fuse_none_when_no_sl_tp():
    """未给 sl/tp → 不发保险丝单（零回归）。"""
    ex, client = _binance_ex()
    spec = VenueOrderSpec(
        symbol="BTCUSDT", side=OrderSide.BUY, offset=Offset.OPEN,
        qty=Decimal("0.10"), kind=OrderKind.MARKET,
    )
    asyncio.run(ex._place_fuse_async(spec))
    assert client.posts == []
