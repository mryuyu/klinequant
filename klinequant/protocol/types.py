"""KlineQuant 核心数据结构定义

所有引擎间通信和数据持久化的基础类型。
遵循需求文档 §5.1 ~ §5.7 及 §12.5 的定义。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────────────
# §5.1 标准化 K 线
# ─────────────────────────────────────────────
@dataclass(frozen=True)
class Kline:
    """标准化 K 线数据结构，所有交易所数据统一为此格式。

    约束:
        - timestamp 必须对齐到周期边界
        - high >= max(open, close), low <= min(open, close)
        - volume >= 0
        - 时区统一为 UTC
    """

    symbol: str
    exchange: str
    timeframe: str
    timestamp: int  # K 线开盘时间，Unix 毫秒时间戳 (UTC)
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    is_closed: bool  # 该 K 线是否已收盘

    def __post_init__(self) -> None:
        if self.high < max(self.open, self.close):
            raise ValueError(
                f"high ({self.high}) must >= max(open, close) = {max(self.open, self.close)}"
            )
        if self.low > min(self.open, self.close):
            raise ValueError(
                f"low ({self.low}) must <= min(open, close) = {min(self.open, self.close)}"
            )
        if self.volume < 0:
            raise ValueError(f"volume ({self.volume}) must >= 0")


# ─────────────────────────────────────────────
# §5.2 Tick 数据
# ─────────────────────────────────────────────
@dataclass(frozen=True)
class Tick:
    """逐笔成交 / 盘口快照"""

    symbol: str
    exchange: str
    timestamp: int  # Unix 毫秒
    last_price: Decimal
    bid_price: Decimal
    bid_qty: Decimal
    ask_price: Decimal
    ask_qty: Decimal
    volume_24h: Decimal


# ─────────────────────────────────────────────
# §5.3 订单
# ─────────────────────────────────────────────
class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_LIMIT = "STOP_LIMIT"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIAL_FILLED = "PARTIAL"
    FILLED = "FILLED"
    CANCELING = "CANCELING"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


# 合法状态流转表
_VALID_TRANSITIONS: Dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.PENDING: {
        OrderStatus.SUBMITTED,
        OrderStatus.FAILED,
        OrderStatus.REJECTED,
    },
    OrderStatus.SUBMITTED: {
        OrderStatus.PARTIAL_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELING,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    },
    OrderStatus.PARTIAL_FILLED: {
        OrderStatus.FILLED,
        OrderStatus.CANCELING,
        OrderStatus.EXPIRED,
    },
    OrderStatus.CANCELING: {
        OrderStatus.CANCELED,
        OrderStatus.FILLED,
        OrderStatus.PARTIAL_FILLED,
    },
    # 终态不允许再流转
    OrderStatus.FILLED: set(),
    OrderStatus.CANCELED: set(),
    OrderStatus.REJECTED: set(),
    OrderStatus.EXPIRED: set(),
    OrderStatus.FAILED: set(),
}


@dataclass
class Order:
    """订单数据结构"""

    order_id: str  # 系统内部唯一 ID (UUID)
    symbol: str
    exchange: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    status: OrderStatus = OrderStatus.PENDING
    exchange_order_id: str = ""
    strategy_id: str = ""
    price: Optional[Decimal] = None  # 限价单价格，市价单为 None
    filled_quantity: Decimal = Decimal("0")
    avg_fill_price: Decimal = Decimal("0")
    created_at: int = 0
    updated_at: int = 0
    filled_at: Optional[int] = None
    cancel_reason: Optional[str] = None
    fee: Decimal = Decimal("0")
    fee_currency: str = ""
    client_order_id: str = ""  # 幂等 ID，防重复下单
    metadata: Dict[str, Any] = field(default_factory=dict)

    def can_transition_to(self, new_status: OrderStatus) -> bool:
        """检查是否可以从当前状态流转到 new_status"""
        return new_status in _VALID_TRANSITIONS.get(self.status, set())

    def transition_to(self, new_status: OrderStatus) -> None:
        """执行状态流转，不合法时抛出 ValueError"""
        if not self.can_transition_to(new_status):
            raise ValueError(
                f"Invalid transition: {self.status.value} -> {new_status.value}"
            )
        self.status = new_status


# ─────────────────────────────────────────────
# §5.4 持仓
# ─────────────────────────────────────────────
@dataclass
class Position:
    symbol: str
    exchange: str
    side: str  # "LONG" / "SHORT" / "FLAT"
    quantity: Decimal
    avg_entry_price: Decimal
    unrealized_pnl: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    margin: Decimal = Decimal("0")
    leverage: int = 1
    updated_at: int = 0


# ─────────────────────────────────────────────
# §5.5 交易信号
# ─────────────────────────────────────────────
class SignalDirection(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    CLOSE = "CLOSE"
    NEUTRAL = "NEUTRAL"


class SignalStrength(int, Enum):
    WEAK = 1
    MEDIUM = 2
    STRONG = 3


@dataclass
class Signal:
    signal_id: str
    strategy_id: str
    symbol: str
    direction: SignalDirection
    strength: SignalStrength
    price: Decimal
    reason: str
    timestamp: int
    suggested_quantity: Optional[Decimal] = None
    indicators: Dict[str, Any] = field(default_factory=dict)
    expires_at: int = 0
    status: str = "PENDING"  # PENDING / CONFIRMED / EXECUTED / EXPIRED / REJECTED

    def is_expired(self, current_time: int) -> bool:
        """信号是否已过期"""
        if self.expires_at == 0:
            return False
        return current_time > self.expires_at


# ─────────────────────────────────────────────
# §5.6 账户
# ─────────────────────────────────────────────
@dataclass
class Account:
    exchange: str
    account_type: str  # "SPOT" / "FUTURES"
    total_balance: Decimal
    available_balance: Decimal
    frozen_balance: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    positions: List[Position] = field(default_factory=list)
    updated_at: int = 0


# ─────────────────────────────────────────────
# §5.7 指标值
# ─────────────────────────────────────────────
@dataclass(frozen=True)
class IndicatorValue:
    indicator_name: str  # 如 "MA", "RSI", "MACD"
    symbol: str
    timeframe: str
    timestamp: int
    values: Dict[str, Any]  # 如 {"MA": 42000.5} 或 {"MACD": {"DIF": 100, "DEA": 80}}
    params: Dict[str, Any] = field(default_factory=dict)


# ─────────────────────────────────────────────
# §12.5 品种信息
# ─────────────────────────────────────────────
@dataclass(frozen=True)
class SymbolInfo:
    symbol: str  # 内部统一标识 "BTC-USDT"
    exchange: str
    base_currency: str  # "BTC"
    quote_currency: str  # "USDT"
    price_precision: int
    qty_precision: int
    min_qty: Decimal
    min_notional: Decimal
    tick_size: Decimal
    market_type: str = "SPOT"  # "SPOT" / "FUTURES" / "SWAP"
    status: str = "ACTIVE"  # "ACTIVE" / "SUSPENDED" / "DELISTED"

    def __post_init__(self) -> None:
        if self.price_precision < 0:
            raise ValueError(f"price_precision ({self.price_precision}) must >= 0")
        if self.qty_precision < 0:
            raise ValueError(f"qty_precision ({self.qty_precision}) must >= 0")
        if self.min_qty <= 0:
            raise ValueError(f"min_qty ({self.min_qty}) must > 0")


# ─────────────────────────────────────────────
# 合约交易扩展（Phase 2）
# ─────────────────────────────────────────────
class MarginMode(str, Enum):
    """保证金模式"""
    CROSS = "CROSS"      # 全仓
    ISOLATED = "ISOLATED"  # 逐仓


class ContractType(str, Enum):
    """合约类型"""
    PERPETUAL = "PERPETUAL"  # 永续合约
    QUARTERLY = "QUARTERLY"  # 交割合约


@dataclass(frozen=True)
class FundingRate:
    """资金费率信息"""
    symbol: str
    exchange: str
    funding_rate: Decimal       # 当前资金费率 (e.g. 0.0001 = 0.01%)
    next_funding_time: int      # 下次结算时间 (Unix ms)
    mark_price: Decimal         # 标记价格
    index_price: Decimal        # 指数价格
    timestamp: int              # 数据时间戳

    @property
    def rate_percent(self) -> Decimal:
        """费率百分比"""
        return self.funding_rate * Decimal("100")

    @property
    def is_positive(self) -> bool:
        """正费率（多头付空头）"""
        return self.funding_rate > Decimal("0")


@dataclass
class FuturesPosition:
    """合约持仓（扩展 Position）"""
    symbol: str
    exchange: str
    side: str                    # "LONG" / "SHORT"
    quantity: Decimal
    avg_entry_price: Decimal
    mark_price: Decimal = Decimal("0")
    liquidation_price: Decimal = Decimal("0")  # 强平价格
    unrealized_pnl: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    margin: Decimal = Decimal("0")             # 占用保证金
    leverage: int = 1
    margin_mode: MarginMode = MarginMode.CROSS
    contract_type: ContractType = ContractType.PERPETUAL
    margin_ratio: Decimal = Decimal("0")       # 保证金率
    updated_at: int = 0

    @property
    def notional_value(self) -> Decimal:
        """名义价值"""
        return self.quantity * self.mark_price

    @property
    def pnl_ratio(self) -> Decimal:
        """收益率"""
        if self.margin == Decimal("0"):
            return Decimal("0")
        return self.unrealized_pnl / self.margin

    @property
    def is_danger(self) -> bool:
        """是否接近强平（保证金率 < 20%）"""
        if self.margin_ratio == Decimal("0"):
            return False
        return self.margin_ratio > Decimal("0.8")


@dataclass(frozen=True)
class FundingFee:
    """资金费结算记录"""
    symbol: str
    exchange: str
    funding_rate: Decimal
    position_qty: Decimal       # 持仓量
    fee_amount: Decimal         # 结算金额 (正=收入, 负=支出)
    funding_time: int           # 结算时间
    strategy_id: str = ""
