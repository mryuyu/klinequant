"""KqApi 多品种支持单测：specs 表 + _spec_for 路由 + symbols()

验证单进程多品种架构下，symbol_info/send_order 按品种取各自 spec
（不同品种 pip/step/digits 不同，绝不混用），支撑「一进程 + 每品种一工作线程」。
"""
from decimal import Decimal

import pytest

from core.trade_engine.ledger import ExposureLedger
from protocol.types import Offset, OrderKind, OrderSide, SymbolInfo
from strategy.sdk.api import KqApi


def _spec(symbol: str, pip: str) -> SymbolInfo:
    return SymbolInfo(
        symbol=symbol, market_type="FX",
        pip_size=Decimal(pip), qty_step=Decimal("0.01"),
        min_qty=Decimal("0.01"), qty_max=Decimal("200"),
    )


class _FakeResolution:
    """最小 Resolution：ok=False 短路，避免触及 executor。"""

    def __init__(self):
        self.ok = False
        self.rejection = None
        self.specs = []


class _FakeResolver:
    """记录 resolve 收到的 (symbol, spec)，用于断言按品种路由。"""

    def __init__(self):
        self.seen = []

    def resolve(self, req, spec, **kwargs):
        self.seen.append((req.symbol, spec))
        return _FakeResolution()


class _FakeExecutor:
    def submit(self, spec):
        raise AssertionError("ok=False 应短路，不该触及 executor.submit")

    def cancel(self, *a, **k):
        return True

    def query_positions(self, symbol=""):
        return []

    def query_account(self):
        return None

    def query_orders(self, symbol=""):
        return []


class _FakeFeed:
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


def _make_api():
    specs = {
        "EURUSD": _spec("EURUSD", "0.0001"),
        "USDJPY": _spec("USDJPY", "0.01"),   # JPY 对 pip 不同
    }
    resolver = _FakeResolver()
    api = KqApi(
        symbol="EURUSD", period="1m", tag="1m",
        specs=specs, ledger=ExposureLedger(), resolver=resolver,
        executor=_FakeExecutor(), feed=_FakeFeed(),
    )
    return api, resolver, specs


def test_symbols_returns_all_in_order():
    """symbols() 返回全部订阅品种（主品种在首位）。"""
    api, _, _ = _make_api()
    assert api.symbols() == ["EURUSD", "USDJPY"]


def test_symbol_info_returns_per_symbol_spec():
    """symbol_info 按品种返回各自 spec，默认回落主品种。"""
    api, _, specs = _make_api()
    assert api.symbol_info() is specs["EURUSD"]
    assert api.symbol_info("USDJPY") is specs["USDJPY"]
    assert api.symbol_info("USDJPY").pip_size == Decimal("0.01")
    assert api.symbol_info("EURUSD").pip_size == Decimal("0.0001")


def test_symbol_info_case_insensitive_fallback():
    """小写品种名经 upper 回落命中。"""
    api, _, specs = _make_api()
    assert api.symbol_info("usdjpy") is specs["USDJPY"]


def test_spec_for_unknown_symbol_raises():
    """未加载的品种取 spec 抛 KeyError（避免静默用错 spec）。"""
    api, _, _ = _make_api()
    with pytest.raises(KeyError):
        api.symbol_info("XXXUSD")


def test_send_order_routes_correct_spec_per_symbol():
    """send_order 对不同品种把各自 spec 传给 resolver（不混用）。"""
    api, resolver, specs = _make_api()
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.01"),
                   kind=OrderKind.MARKET, symbol="EURUSD")
    api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.01"),
                   kind=OrderKind.MARKET, symbol="USDJPY")
    assert resolver.seen[0] == ("EURUSD", specs["EURUSD"])
    assert resolver.seen[1] == ("USDJPY", specs["USDJPY"])


def test_positions_isolated_per_symbol():
    """账本按 (symbol, tag) 隔离：一个品种持仓不影响另一个。"""
    api, _, _ = _make_api()
    api._ledger.sync_from_venue("EURUSD", "1m", Decimal("0.01"), Decimal("1.14"))
    assert api.position("EURUSD").volume == Decimal("0.01")
    assert api.position("USDJPY").volume == Decimal("0")
