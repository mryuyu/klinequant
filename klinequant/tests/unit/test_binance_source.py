"""币安市场源插件单测（纯解析逻辑，不依赖网络）"""
from gateway.market_sources.binance_source import BinanceSource


def test_parse_exchange_info_filters_trading_usdt():
    """全量目录：仅保留 TRADING + USDT 计价 + 允许现货交易的品种"""
    info = {"symbols": [
        {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT",
         "status": "TRADING", "isSpotTradingAllowed": True},
        {"symbol": "ETHBTC", "baseAsset": "ETH", "quoteAsset": "BTC",
         "status": "TRADING"},                        # 非 USDT 计价
        {"symbol": "OLDUSDT", "baseAsset": "OLD", "quoteAsset": "USDT",
         "status": "BREAK"},                          # 已停牌
        {"symbol": "LEVERUSDT", "baseAsset": "LEVER", "quoteAsset": "USDT",
         "status": "TRADING", "isSpotTradingAllowed": False},   # 杠杆代币
    ]}
    rows = BinanceSource._parse_exchange_info(info)
    assert rows == [{"symbol": "BTCUSDT", "name": "BTC/USDT", "type": "crypto"}]


def test_parse_exchange_info_empty():
    assert BinanceSource._parse_exchange_info({}) == []
