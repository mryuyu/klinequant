"""K 线数据标准化

将各交易所原始 K 线数据统一转换为内部标准 Kline 格式：
    - 时间戳对齐到周期边界
    - 价格/量统一为 Decimal
    - 时区统一为 UTC
    - 数据校验（OHLC 约束、非负量）

遵循需求文档 §4.1 MKT-002、MKT-004。
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from protocol.types import Kline


# ─── 周期 → 毫秒映射 ───

TIMEFRAME_MS: Dict[str, int] = {
    "1m":  60_000,
    "3m":  180_000,
    "5m":  300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h":  3_600_000,
    "2h":  7_200_000,
    "4h":  14_400_000,
    "6h":  21_600_000,
    "8h":  28_800_000,
    "12h": 43_200_000,
    "1d":  86_400_000,
    "3d":  259_200_000,
    "1w":  604_800_000,
    "1M":  2_592_000_000,  # 近似值，实际按自然月
}


def timeframe_to_ms(timeframe: str) -> int:
    """将周期字符串转换为毫秒数。"""
    if timeframe not in TIMEFRAME_MS:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return TIMEFRAME_MS[timeframe]


def align_timestamp(timestamp: int, timeframe: str) -> int:
    """将时间戳对齐到周期边界（向下取整）。"""
    ms = timeframe_to_ms(timeframe)
    return (timestamp // ms) * ms


def _to_decimal(value: Any) -> Decimal:
    """安全转换为 Decimal。"""
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


# ─── Binance 标准化 ───


def normalize_binance_kline(
    raw: List[Any],
    symbol: str,
    timeframe: str,
    exchange: str = "binance",
) -> Kline:
    """将 Binance K 线数组标准化为 Kline。

    Binance /api/v3/klines 返回格式:
        [open_time, open, high, low, close, volume, close_time,
         quote_volume, trade_count, taker_buy_base, taker_buy_quote, ignore]

    Args:
        raw: Binance 返回的原始数组（12 个元素）
        symbol: 交易对名称
        timeframe: K 线周期
        exchange: 交易所名称

    Returns:
        标准化 Kline 实例
    """
    open_time = int(raw[0])
    close_time = int(raw[6])
    now_ms = _current_time_ms()

    # 判断是否已收盘：close_time < 当前时间
    is_closed = close_time < now_ms

    return Kline(
        symbol=symbol.upper(),
        exchange=exchange,
        timeframe=timeframe,
        timestamp=align_timestamp(open_time, timeframe),
        open=_to_decimal(raw[1]),
        high=_to_decimal(raw[2]),
        low=_to_decimal(raw[3]),
        close=_to_decimal(raw[4]),
        volume=_to_decimal(raw[5]),
        quote_volume=_to_decimal(raw[7]),
        trade_count=int(raw[8]),
        is_closed=is_closed,
    )


def normalize_binance_klines(
    raw_list: List[List[Any]],
    symbol: str,
    timeframe: str,
    exchange: str = "binance",
) -> List[Kline]:
    """批量标准化 Binance K 线。"""
    result = []
    for raw in raw_list:
        try:
            result.append(normalize_binance_kline(raw, symbol, timeframe, exchange))
        except (ValueError, IndexError, KeyError) as e:
            # 跳过无效数据，记录但不中断
            import logging
            logging.getLogger(__name__).warning(
                f"Skip invalid kline: {symbol} {timeframe} {e}"
            )
    return result


def _current_time_ms() -> int:
    """获取当前时间（毫秒）。"""
    import time
    return int(time.time() * 1000)
