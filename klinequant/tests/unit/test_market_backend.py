"""MarketBackend 抽象 + LiveRunner 注入单元测试

- Mt5Backend：mock driver/executor/feed，注入 LiveRunner 全流程跑通，验证
  backend 委托顺序、多品种各一 KqApi、退出 flatten 清仓、shutdown。
- BinanceBackend：真实 event loop 线程 + mock client/adapter，验证 connect
  （One-way + 杠杆）、load_specs、make_executor/make_feed、reconcile、shutdown。
证明 LiveRunner 已市场无关，换 backend 即换市场（同构）。
"""
import asyncio
import threading
from decimal import Decimal
from unittest import mock

import pytest

from core.trade_engine.executors.mt5_executor import SubmitResult
from core.trade_engine.ledger import ExposureLedger
from protocol.types import Offset, SymbolInfo
from strategy.sdk.backend import BinanceBackend, MarketBackend, Mt5Backend
from strategy.sdk.live_runner import LiveRunner


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


# ═══════════════════════════════════════════
# Mt5Backend 注入 LiveRunner（全流程委托）
# ═══════════════════════════════════════════

def _run_mt5_runner(symbols, positions):
    """用 mock driver/executor/feed 跑一遍 LiveRunner，返回 (backend, executor, feed, seen)"""
    seen = []

    def strategy(api):
        seen.append((api._symbol, tuple(api.symbols())))
        # 立即退出（wait_update 返回 False）→ 线程结束 → 触发 shutdown flatten

    with mock.patch("strategy.sdk.backend.Mt5Api") as MockApi, \
         mock.patch("strategy.sdk.backend.load_spec_from_mt5") as mock_spec, \
         mock.patch("strategy.sdk.backend.Mt5Executor") as MockExec, \
         mock.patch("strategy.sdk.backend.Mt5DataFeed") as MockFeed:

        driver = MockApi.return_value
        driver.initialize.return_value = True
        mock_spec.side_effect = lambda drv, sym: _fx_spec(sym)

        executor = MockExec.return_value
        executor.query_positions.side_effect = lambda sym="": [
            p for p in positions if p.get("symbol") == sym
        ]
        executor.query_orders.return_value = []
        executor.query_account.return_value = {
            "balance": 10000.0, "equity": 10000.0, "margin": 0.0,
            "margin_free": 10000.0, "profit": 0.0, "currency": "USD",
        }
        executor.submit.return_value = SubmitResult(
            success=True, status="FILLED", order_ticket=1,
            filled_qty=Decimal("0.01"), filled_price=Decimal("1.10"),
        )

        feed = MockFeed.return_value
        feed.wait_update.return_value = False
        feed.latest_bars.return_value = []

        backend = Mt5Backend(magic=202609, deviation=20)
        runner = LiveRunner(backend, symbols=symbols, period="1m",
                            strategy_fn=strategy, bar_count=300, poll_interval=0.5)
        runner.run()

    return backend, executor, feed, seen


def test_mt5_backend_delegation_order():
    backend, executor, feed, seen = _run_mt5_runner(["EURUSD"], positions=[])
    # 委托：driver 由 backend.connect 初始化，并在退出 shutdown 关闭
    assert backend.driver is not None
    assert backend.driver.initialize.called
    assert backend.driver.shutdown.called
    # 数据源由 backend.make_feed 构造并启动；策略跑过一份
    assert feed.start.called
    assert len(seen) == 1


def test_mt5_multi_symbol_one_api_each():
    backend, executor, feed, seen = _run_mt5_runner(["EURUSD", "GBPUSD"], positions=[])
    syms = sorted(s for s, _ in seen)
    assert syms == ["EURUSD", "GBPUSD"]          # 每品种各跑一份策略
    for _sym, all_syms in seen:
        assert list(all_syms) == ["EURUSD", "GBPUSD"]   # 每个 KqApi 见到全部品种


def test_mt5_flatten_on_shutdown_closes_net_position():
    positions = [
        {"ticket": 1, "symbol": "EURUSD", "type": 0, "volume": 0.01,
         "price_open": 1.10, "time": 100},
    ]
    backend, executor, feed, seen = _run_mt5_runner(["EURUSD"], positions)
    # 退出清仓应提交一笔 CLOSE
    close_specs = [
        c.args[0] for c in executor.submit.call_args_list
        if c.args and c.args[0].offset == Offset.CLOSE
    ]
    assert len(close_specs) >= 1
    assert close_specs[0].side.value in ("BUY", "SELL")
    assert close_specs[0].qty == Decimal("0.01")


def test_mt5_reconcile_syncs_ledger():
    """启动对账把 venue 净持仓写回 ledger（EURUSD 多头 0.02）"""
    positions = [
        {"ticket": 1, "symbol": "EURUSD", "type": 0, "volume": 0.02,
         "price_open": 1.10, "time": 100},
    ]
    backend, executor, feed, seen = _run_mt5_runner(["EURUSD"], positions)
    # reconcile 后 flatten 平了 0.02（提交 CLOSE qty=0.02）
    close_specs = [
        c.args[0] for c in executor.submit.call_args_list
        if c.args and c.args[0].offset == Offset.CLOSE
    ]
    assert close_specs and close_specs[0].qty == Decimal("0.02")


# ═══════════════════════════════════════════
# BinanceBackend（真实 loop 线程 + mock client/adapter）
# ═══════════════════════════════════════════

class _Resp:
    def __init__(self, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json = json_data if json_data is not None else {}
        self.text = text
        self.headers = {"content-type": "application/json"}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.text)


def test_binance_backend_satisfies_protocol():
    assert isinstance(BinanceBackend(["BTCUSDT"]), MarketBackend)
    assert isinstance(Mt5Backend(), MarketBackend)


def test_binance_load_specs():
    backend = BinanceBackend(["BTCUSDT"], api_key="k", api_secret="s")
    backend._ensure_loop()
    backend._client = mock.Mock()
    backend._client.get = mock.AsyncMock(return_value=_Resp(200, {
        "symbols": [{
            "symbol": "BTCUSDT", "status": "TRADING", "baseAsset": "BTC",
            "quoteAsset": "USDT", "pricePrecision": 2, "quantityPrecision": 3,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
            ],
        }],
    }))
    try:
        specs = backend.load_specs(["BTCUSDT"])
        assert "BTCUSDT" in specs
        assert specs["BTCUSDT"].market_type == "FUTURES"
        assert specs["BTCUSDT"].qty_step == Decimal("0.001")
        assert specs["BTCUSDT"].close_priority == "net"
    finally:
        backend.shutdown()


def test_binance_make_executor_and_feed():
    from core.trade_engine.executors.binance_executor import BinanceExecutor
    from strategy.sdk.binance_feed import BinanceDataFeed

    backend = BinanceBackend(["BTCUSDT"], api_key="k", api_secret="s")
    backend._ensure_loop()
    backend._client = mock.Mock()
    backend._adapter = mock.Mock()
    try:
        ex = backend.make_executor()
        assert isinstance(ex, BinanceExecutor)
        assert ex._loop is backend._loop
        assert ex._client is backend._client

        feed = backend.make_feed(["BTCUSDT"], ["1m"], 0.5, 300)
        assert isinstance(feed, BinanceDataFeed)
        assert feed._loop is backend._loop
    finally:
        backend.shutdown()


def test_binance_reconcile_net_position():
    backend = BinanceBackend(["BTCUSDT"])
    ledger = ExposureLedger()

    class _StubExec:
        def query_positions(self, symbol=""):
            if symbol != "BTCUSDT":
                return []
            return [{"type": 0, "volume": 0.5, "price_open": 50000.0}]

    backend.reconcile_positions(_StubExec(), ledger, ["BTCUSDT"], "1m")
    assert ledger.net_position("BTCUSDT") == Decimal("0.5")


def test_binance_connect_sets_one_way_and_leverage():
    backend = BinanceBackend(
        ["BTCUSDT"], api_key="k", api_secret="s", proxy=None, leverage=3,
    )
    with mock.patch("httpx.AsyncClient") as MockClient, \
         mock.patch("core.market_engine.adapters.binance_futures.BinanceFuturesAdapter") as MockAdapter:

        adapter = MockAdapter.return_value
        adapter.connect = mock.AsyncMock()
        adapter.set_leverage = mock.AsyncMock(return_value=3)
        adapter.disconnect = mock.AsyncMock()

        client = MockClient.return_value
        client.post = mock.AsyncMock(return_value=_Resp(200, {"code": 0, "msg": "success"}))
        client.aclose = mock.AsyncMock()

        backend.connect()
        try:
            adapter.connect.assert_awaited()
            # One-way：POST /fapi/v1/positionSide/dual
            post_paths = [c.args[0] for c in client.post.call_args_list]
            assert "/fapi/v1/positionSide/dual" in post_paths
            dual_kwargs = client.post.call_args_list[0].kwargs["params"]
            assert dual_kwargs["dualSidePosition"] == "false"
            # 杠杆
            adapter.set_leverage.assert_awaited_with("BTCUSDT", 3)
        finally:
            backend.shutdown()

    assert backend._loop is None   # shutdown 已停并关闭 loop


def test_binance_shutdown_closes_adapter_and_client():
    backend = BinanceBackend(["BTCUSDT"], api_key="k", api_secret="s", proxy=None)
    with mock.patch("httpx.AsyncClient") as MockClient, \
         mock.patch("core.market_engine.adapters.binance_futures.BinanceFuturesAdapter") as MockAdapter:
        adapter = MockAdapter.return_value
        adapter.connect = mock.AsyncMock()
        adapter.set_leverage = mock.AsyncMock(return_value=1)
        adapter.disconnect = mock.AsyncMock()
        client = MockClient.return_value
        client.post = mock.AsyncMock(return_value=_Resp(200, {}))
        client.aclose = mock.AsyncMock()

        backend.connect()
        backend.shutdown()

        adapter.disconnect.assert_awaited()
        client.aclose.assert_awaited()
    assert backend._loop is None
