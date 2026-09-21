"""load_spec_from_binance 单元测试

验证从币安 exchangeInfo 的单品种条目解析 SymbolInfo：filters（LOT_SIZE /
PRICE_FILTER / MIN_NOTIONAL）→ 量化字段，market_type=FUTURES，One-way net。
"""
from decimal import Decimal

from core.trade_engine.spec_loader import load_spec_from_binance


def _btc_info():
    return {
        "symbol": "BTCUSDT",
        "status": "TRADING",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "pricePrecision": 2,
        "quantityPrecision": 3,
        "filters": [
            {"filterType": "PRICE_FILTER", "minPrice": "0.01", "tickSize": "0.10"},
            {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
            {"filterType": "MIN_NOTIONAL", "notional": "5"},
        ],
    }


def test_parse_filters_to_quantization_fields():
    spec = load_spec_from_binance(_btc_info(), "BTCUSDT")
    assert spec.symbol == "BTCUSDT"
    assert spec.exchange == "binance_futures"
    assert spec.base_currency == "BTC"
    assert spec.quote_currency == "USDT"
    assert spec.qty_step == Decimal("0.001")
    assert spec.min_qty == Decimal("0.001")
    assert spec.qty_max == Decimal("1000")
    assert spec.tick_size == Decimal("0.10")
    assert spec.min_notional == Decimal("5")


def test_futures_market_metadata():
    spec = load_spec_from_binance(_btc_info(), "BTCUSDT")
    assert spec.market_type == "FUTURES"
    assert spec.qty_unit == "COIN"
    assert spec.contract_multiplier == Decimal("1")
    assert spec.can_short is True
    assert spec.close_priority == "net"
    assert spec.t_plus_n == 0
    assert spec.price_precision == 2
    assert spec.qty_precision == 3
    assert spec.status == "ACTIVE"


def test_precision_falls_back_to_filter_places():
    """缺 pricePrecision/quantityPrecision 时用 tick/step 的小数位兜底"""
    info = _btc_info()
    info.pop("pricePrecision")
    info.pop("quantityPrecision")
    spec = load_spec_from_binance(info, "BTCUSDT")
    assert spec.price_precision == 2   # tickSize 0.10 → 2 位小数
    assert spec.qty_precision == 3     # stepSize 0.001 → 3 位


def test_suspended_when_not_trading():
    info = _btc_info()
    info["status"] = "SETTLING"
    spec = load_spec_from_binance(info, "BTCUSDT")
    assert spec.status == "SUSPENDED"


def test_spot_style_min_notional_fallback():
    """MIN_NOTIONAL 用旧字段 minNotional 时也能解析"""
    info = _btc_info()
    info["filters"][2] = {"filterType": "MIN_NOTIONAL", "minNotional": "100"}
    spec = load_spec_from_binance(info, "BTCUSDT")
    assert spec.min_notional == Decimal("100")
