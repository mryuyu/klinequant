"""OKX 数据标准化

将 OKX 原始 K 线/Tick 数据统一转换为内部标准格式。

OKX API v5 数据格式：
    - K 线 (REST): [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    - K 线 (WS): 同上，通过 candle{bar} 频道推送
    - 成交: {instId, tradeId, px, sz, side, ts}

遵循需求文档 §4.1 MKT-002。
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List

from protocol.types import Kline, Tick
from core.market_engine.normalizer import align_timestamp, _to_decimal


# ─── OKX 周期映射 ───
# 内部标准周期 → OKX bar 参数
TIMEFRAME_TO_OKX: Dict[str, str] = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "2h": "2H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "1w": "1W",
    "1M": "1M",
}

# OKX bar 参数 → 内部标准周期
OKX_TO_TIMEFRAME: Dict[str, str] = {v: k for k, v in TIMEFRAME_TO_OKX.items()}
# 补充 UTC 变体
OKX_TO_TIMEFRAME.update({
    "1Dutc": "1d",
    "1Wutc": "1w",
    "1Mutc": "1M",
})


def timeframe_to_okx_bar(timeframe: str) -> str:
    """将内部周期转换为 OKX bar 参数。"""
    if timeframe not in TIMEFRAME_TO_OKX:
        raise ValueError(f"Unsupported timeframe for OKX: {timeframe}")
    return TIMEFRAME_TO_OKX[timeframe]


def okx_bar_to_timeframe(bar: str) -> str:
    """将 OKX bar 参数转换为内部周期。"""
    if bar not in OKX_TO_TIMEFRAME:
        raise ValueError(f"Unknown OKX bar: {bar}")
    return OKX_TO_TIMEFRAME[bar]


# ─── 交易对标准化 ───

def normalize_symbol(internal_symbol: str) -> str:
    """内部交易对 → OKX 格式。

    内部: "BTCUSDT" → OKX: "BTC-USDT"
    如果已含 "-" 则原样返回。
    """
    if "-" in internal_symbol:
        return internal_symbol.upper()

    # 常见计价货币后缀
    quote_currencies = ["USDT", "USDC", "BTC", "ETH", "USD"]
    upper = internal_symbol.upper()
    for quote in quote_currencies:
        if upper.endswith(quote) and len(upper) > len(quote):
            base = upper[: -len(quote)]
            return f"{base}-{quote}"

    # 无法识别，原样返回
    return upper


def denormalize_symbol(okx_symbol: str) -> str:
    """OKX 格式 → 内部格式。

    OKX: "BTC-USDT" → 内部: "BTCUSDT"
    """
    return okx_symbol.replace("-", "").upper()


# ─── K 线标准化 ───

def normalize_okx_kline(
    raw: List[Any],
    symbol: str,
    timeframe: str,
    exchange: str = "okx",
) -> Kline:
    """将 OKX K 线数组标准化为 Kline。

    OKX /api/v5/market/candles 返回格式:
        [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]

    Args:
        raw: OKX 返回的原始数组（9 个元素）
        symbol: 交易对名称（内部格式，如 "BTCUSDT"）
        timeframe: K 线周期（内部格式，如 "1m"）
        exchange: 交易所名称

    Returns:
        标准化 Kline 实例
    """
    ts = int(raw[0])
    # confirm: "0" = 未收盘, "1" = 已收盘
    confirm = str(raw[8]) if len(raw) > 8 else "0"
    is_closed = confirm == "1"

    return Kline(
        symbol=symbol.upper(),
        exchange=exchange,
        timeframe=timeframe,
        timestamp=align_timestamp(ts, timeframe),
        open=_to_decimal(raw[1]),
        high=_to_decimal(raw[2]),
        low=_to_decimal(raw[3]),
        close=_to_decimal(raw[4]),
        volume=_to_decimal(raw[5]),       # 基础货币成交量
        quote_volume=_to_decimal(raw[7]),  # 计价货币成交额
        trade_count=0,                     # OKX K 线不提供成交笔数
        is_closed=is_closed,
    )


def normalize_okx_klines(
    raw_list: List[List[Any]],
    symbol: str,
    timeframe: str,
    exchange: str = "okx",
) -> List[Kline]:
    """批量标准化 OKX K 线。

    注意：OKX REST 返回的 K 线是按时间降序（最新在前），
    此函数会反转为升序。
    """
    import logging
    result = []
    for raw in raw_list:
        try:
            result.append(normalize_okx_kline(raw, symbol, timeframe, exchange))
        except (ValueError, IndexError, KeyError) as e:
            logging.getLogger(__name__).warning(
                f"Skip invalid OKX kline: {symbol} {timeframe} {e}"
            )
    # OKX 返回降序，反转为升序
    result.reverse()
    return result


# ─── Tick 标准化 ───

def normalize_okx_trade(
    data: Dict[str, Any],
    exchange: str = "okx",
) -> Tick:
    """将 OKX 成交数据标准化为 Tick。

    OKX trades 频道推送格式:
        {instId, tradeId, px, sz, side, ts}
    """
    price = _to_decimal(data.get("px", "0"))
    size = _to_decimal(data.get("sz", "0"))
    inst_id = data.get("instId", "")

    return Tick(
        symbol=denormalize_symbol(inst_id),
        exchange=exchange,
        timestamp=int(data.get("ts", 0)),
        last_price=price,
        bid_price=price,
        bid_qty=size if data.get("side") == "buy" else Decimal("0"),
        ask_price=price,
        ask_qty=size if data.get("side") == "sell" else Decimal("0"),
        volume_24h=Decimal("0"),  # 单笔成交无 24h 量
    )


def normalize_okx_ticker(
    data: Dict[str, Any],
    exchange: str = "okx",
) -> Tick:
    """将 OKX ticker 数据标准化为 Tick。

    OKX tickers 频道推送格式:
        {instId, last, lastSz, askPx, askSz, bidPx, bidSz, vol24h, ts, ...}
    """
    return Tick(
        symbol=denormalize_symbol(data.get("instId", "")),
        exchange=exchange,
        timestamp=int(data.get("ts", 0)),
        last_price=_to_decimal(data.get("last", "0")),
        bid_price=_to_decimal(data.get("bidPx", "0")),
        bid_qty=_to_decimal(data.get("bidSz", "0")),
        ask_price=_to_decimal(data.get("askPx", "0")),
        ask_qty=_to_decimal(data.get("askSz", "0")),
        volume_24h=_to_decimal(data.get("vol24h", "0")),
    )
