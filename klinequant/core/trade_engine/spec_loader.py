"""InstrumentSpec 加载器 — 从 venue API 实测加载品种规格

核心原则：所有字段从 venue 实测获取，不硬编码。
外汇 lot 的 contract_multiplier 因经纪商/品种而异，必须读 mt5.symbol_info()。
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional

from protocol.types import SymbolInfo

logger = logging.getLogger(__name__)


def load_spec_from_mt5(driver, symbol: str) -> Optional[SymbolInfo]:
    """从 MT5 终端加载品种规格。

    Args:
        driver: Mt5Api 实例（需已 initialize）
        symbol: 品种代码（如 "EURUSD"）

    Returns:
        SymbolInfo 实例，加载失败返回 None
    """
    info = driver.symbol_info(symbol)
    if info is None:
        logger.error(f"MT5 symbol_info returned None for {symbol}")
        return None

    digits = int(info.get("digits", 0))
    point = Decimal(str(info.get("point", 0)))

    # pip_size 推导：3/5 位报价平台的 pip = point * 10，其余 pip = point
    # EURUSD 5位: point=0.00001, pip=0.0001
    # USDJPY 3位: point=0.001, pip=0.01
    # XAUUSD 2位: point=0.01, pip=0.01
    if digits in (3, 5):
        pip_size = point * 10
    else:
        pip_size = point

    # 交易合约大小（1 lot = 多少 base currency）
    # EURUSD 标准手: 100000; mini: 10000; XAUUSD: 100 盎司
    contract_size = Decimal(str(info.get("trade_contract_size", 0)))

    # 成交量约束
    volume_step = Decimal(str(info.get("volume_step", 0)))
    volume_min = Decimal(str(info.get("volume_min", 0)))
    volume_max = Decimal(str(info.get("volume_max", 0)))

    # 交易模式：0=disabled, 1=long_only, 2=short_only, 3=both, 4=full
    trade_mode = int(info.get("trade_mode", 0))
    can_short = trade_mode in (2, 3, 4)

    # 点差（points）
    spread_points = int(info.get("spread", 0))

    # 保证金货币 & 计算方式
    margin_currency = info.get("margin_currency", "USD")

    return SymbolInfo(
        symbol=symbol,
        exchange="mt5",
        base_currency=info.get("currency_base", ""),
        quote_currency=info.get("currency_profit", margin_currency),
        price_precision=digits,
        qty_precision=_decimal_places(volume_step),
        min_qty=volume_min,
        min_notional=Decimal("0"),  # FX 无最小名义额概念
        tick_size=point,
        market_type="FX",
        status="ACTIVE" if trade_mode > 0 else "SUSPENDED",
        # v2 扩展
        qty_unit="LOT",
        qty_step=volume_step,
        qty_max=volume_max,
        pip_size=pip_size,
        contract_multiplier=contract_size,
        can_short=can_short,
        t_plus_n=0,  # FX T+0
        close_priority="net",  # IC Markets Netting 账户
    )


def load_spec_from_mt5_dict(info: dict, symbol: str) -> SymbolInfo:
    """从已有的 symbol_info dict 构造 SymbolInfo（测试/缓存场景）。

    与 load_spec_from_mt5 逻辑一致，但跳过 driver 调用。
    """
    digits = int(info.get("digits", 0))
    point = Decimal(str(info.get("point", 0)))
    pip_size = point * 10 if digits in (3, 5) else point
    contract_size = Decimal(str(info.get("trade_contract_size", 0)))
    volume_step = Decimal(str(info.get("volume_step", 0)))
    volume_min = Decimal(str(info.get("volume_min", 0)))
    volume_max = Decimal(str(info.get("volume_max", 0)))
    trade_mode = int(info.get("trade_mode", 0))

    return SymbolInfo(
        symbol=symbol,
        exchange="mt5",
        base_currency=info.get("currency_base", ""),
        quote_currency=info.get("currency_profit", ""),
        price_precision=digits,
        qty_precision=_decimal_places(volume_step),
        min_qty=volume_min,
        min_notional=Decimal("0"),
        tick_size=point,
        market_type="FX",
        status="ACTIVE" if trade_mode > 0 else "SUSPENDED",
        qty_unit="LOT",
        qty_step=volume_step,
        qty_max=volume_max,
        pip_size=pip_size,
        contract_multiplier=contract_size,
        can_short=trade_mode in (2, 3, 4),
        t_plus_n=0,
        close_priority="net",
    )


def _decimal_places(d: Decimal) -> int:
    """计算 Decimal 的小数位数（用于 qty_precision）"""
    if d == 0:
        return 0
    sign, digits, exponent = d.as_tuple()
    return max(0, -exponent)
