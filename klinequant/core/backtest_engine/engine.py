"""BacktestEngine — 回测引擎主循环

核心流程：
    1. 加载历史 K 线数据
    2. 逐 bar 遍历：
       a. 更新指标
       b. 生成信号（策略逻辑）
       c. 信号 → 订单（下根 bar 执行，防 look-ahead bias）
       d. 撮合
       e. 更新持仓和权益
    3. 绩效分析
    4. 结果存储

遵循需求文档 §4.5 BT-005~BT-007。
"""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

import polars as pl

from core.backtest_engine.fee import FeeModel, PercentageFee
from core.backtest_engine.matcher import BacktestFill, Matcher
from core.backtest_engine.performance import (
    PerformanceAnalyzer,
    PerformanceReport,
    Trade,
)
from core.backtest_engine.slippage import PercentageSlippage, SlippageModel
from protocol.types import Kline, Order, OrderSide, OrderStatus, OrderType

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """回测配置"""

    symbol: str = "BTCUSDT"
    exchange: str = "binance"
    timeframe: str = "1h"
    initial_capital: Decimal = Decimal("100000")
    position_size_pct: Decimal = Decimal("0.95")  # 每次开仓使用资金比例
    slippage_model: str = "percentage"
    slippage_params: Dict[str, Any] = field(default_factory=dict)
    fee_model: str = "percentage"
    fee_params: Dict[str, Any] = field(default_factory=dict)
    bars_per_year: int = 8760  # 1h = 8760 bars/year


@dataclass
class BacktestResult:
    """回测结果"""

    backtest_id: str
    config: BacktestConfig
    report: PerformanceReport
    trades: List[Trade]
    fills: List[BacktestFill]
    equity_curve: List[float]
    duration_ms: int = 0
    created_at: int = 0

    def summary(self) -> str:
        r = self.report
        return (
            f"=== Backtest {self.backtest_id[:8]} ===\n"
            f"Symbol: {self.config.symbol} | TF: {self.config.timeframe}\n"
            f"Period: {len(self.equity_curve)} bars\n"
            f"Total Return: {r.total_return:.2%}\n"
            f"Annual Return: {r.annual_return:.2%}\n"
            f"Max Drawdown: {r.max_drawdown:.2%}\n"
            f"Sharpe Ratio: {r.sharpe_ratio:.3f}\n"
            f"Win Rate: {r.win_rate:.2%}\n"
            f"Profit Factor: {r.profit_factor:.3f}\n"
            f"Total Trades: {r.total_trades}\n"
            f"Total Fees: {r.total_fees:.2f}\n"
            f"Duration: {self.duration_ms}ms\n"
        )


# 策略回调签名：接收 DataFrame（含指标列）和当前 bar 索引，返回信号
# 返回: "LONG" / "SHORT" / "CLOSE" / None
StrategyCallback = Callable[[pl.DataFrame, int], Optional[str]]


class BacktestEngine:
    """回测引擎

    使用方式：
        engine = BacktestEngine(config)
        result = engine.run(klines_df, strategy_fn)
        print(result.summary())
    """

    def __init__(self, config: Optional[BacktestConfig] = None):
        self._config = config or BacktestConfig()
        self._matcher: Optional[Matcher] = None
        self._analyzer = PerformanceAnalyzer(bars_per_year=self._config.bars_per_year)

    @property
    def config(self) -> BacktestConfig:
        return self._config

    def run(
        self,
        data: pl.DataFrame,
        strategy: StrategyCallback,
        indicator_fn: Optional[Callable[[pl.DataFrame], pl.DataFrame]] = None,
    ) -> BacktestResult:
        """执行回测

        Args:
            data: K 线 DataFrame，必须包含列:
                  timestamp, open, high, low, close, volume
            strategy: 策略回调函数
            indicator_fn: 指标计算函数（可选，在每根 bar 前调用）

        Returns:
            BacktestResult 回测结果
        """
        start_time = int(time.time() * 1000)

        # 初始化组件
        slippage = self._create_slippage()
        fee = self._create_fee()
        self._matcher = Matcher(slippage_model=slippage, fee_model=fee)

        # 状态
        capital = float(self._config.initial_capital)
        position_qty: float = 0.0
        position_side: Optional[str] = None  # LONG / SHORT
        entry_price: float = 0.0
        entry_time: int = 0
        entry_bar: int = 0
        total_fee: float = 0.0

        equity_curve: List[float] = [capital]
        trades: List[Trade] = []
        all_fills: List[BacktestFill] = []

        n_bars = len(data)

        for i in range(n_bars):
            # 获取当前 bar 数据
            bar = data.slice(i, 1)
            close_price = float(bar["close"][0])
            timestamp = int(bar["timestamp"][0])

            # 1. 撮合之前提交的订单（用当前 bar）
            kline = self._row_to_kline(data, i)
            fills = self._matcher.on_bar(kline, i)

            for fill in fills:
                all_fills.append(fill)
                fee_val = float(fill.fee)
                total_fee += fee_val

                if fill.side == "BUY":
                    if position_side == "SHORT" and position_qty > 0:
                        # 平空
                        pnl = (entry_price - float(fill.price)) * position_qty - fee_val
                        capital += pnl
                        trades.append(Trade(
                            symbol=self._config.symbol,
                            side="SHORT",
                            entry_price=Decimal(str(entry_price)),
                            exit_price=fill.price,
                            quantity=Decimal(str(position_qty)),
                            entry_time=entry_time,
                            exit_time=timestamp,
                            pnl=Decimal(str(pnl)),
                            fee=Decimal(str(fee_val)),
                            bars_held=i - entry_bar,
                        ))
                        position_qty = 0.0
                        position_side = None
                    else:
                        # 开多
                        position_side = "LONG"
                        position_qty = float(fill.quantity)
                        entry_price = float(fill.price)
                        entry_time = timestamp
                        entry_bar = i
                        capital -= fee_val

                elif fill.side == "SELL":
                    if position_side == "LONG" and position_qty > 0:
                        # 平多
                        pnl = (float(fill.price) - entry_price) * position_qty - fee_val
                        capital += pnl
                        trades.append(Trade(
                            symbol=self._config.symbol,
                            side="LONG",
                            entry_price=Decimal(str(entry_price)),
                            exit_price=fill.price,
                            quantity=Decimal(str(position_qty)),
                            entry_time=entry_time,
                            exit_time=timestamp,
                            pnl=Decimal(str(pnl)),
                            fee=Decimal(str(fee_val)),
                            bars_held=i - entry_bar,
                        ))
                        position_qty = 0.0
                        position_side = None
                    else:
                        # 开空
                        position_side = "SHORT"
                        position_qty = float(fill.quantity)
                        entry_price = float(fill.price)
                        entry_time = timestamp
                        entry_bar = i
                        capital -= fee_val

            # 2. 计算当前权益
            unrealized = 0.0
            if position_side == "LONG" and position_qty > 0:
                unrealized = (close_price - entry_price) * position_qty
            elif position_side == "SHORT" and position_qty > 0:
                unrealized = (entry_price - close_price) * position_qty
            equity = capital + unrealized
            equity_curve.append(equity)

            # 3. 策略信号（在收盘时产生）
            # 准备策略数据（到当前 bar 为止）
            strategy_data = data.slice(0, i + 1)
            if indicator_fn:
                strategy_data = indicator_fn(strategy_data)

            signal = strategy(strategy_data, i)

            # 4. 信号 → 订单（下根 bar 执行）
            if signal and i < n_bars - 1:
                self._process_signal(
                    signal, i, close_price, capital, position_side
                )

        # 强制平仓（回测结束时）
        if position_qty > 0 and n_bars > 0:
            last_close = float(data["close"][n_bars - 1])
            last_ts = int(data["timestamp"][n_bars - 1])
            if position_side == "LONG":
                pnl = (last_close - entry_price) * position_qty
            else:
                pnl = (entry_price - last_close) * position_qty
            capital += pnl
            trades.append(Trade(
                symbol=self._config.symbol,
                side=position_side,
                entry_price=Decimal(str(entry_price)),
                exit_price=Decimal(str(last_close)),
                quantity=Decimal(str(position_qty)),
                entry_time=entry_time,
                exit_time=last_ts,
                pnl=Decimal(str(pnl)),
                fee=Decimal("0"),
                bars_held=n_bars - 1 - entry_bar,
            ))
            equity_curve[-1] = capital

        # 绩效分析
        report = self._analyzer.analyze(equity_curve, trades, float(self._config.initial_capital))

        end_time = int(time.time() * 1000)

        return BacktestResult(
            backtest_id=str(uuid.uuid4()),
            config=self._config,
            report=report,
            trades=trades,
            fills=all_fills,
            equity_curve=equity_curve,
            duration_ms=end_time - start_time,
            created_at=end_time,
        )

    def _process_signal(
        self,
        signal: str,
        bar_index: int,
        close_price: float,
        capital: float,
        position_side: Optional[str],
    ) -> None:
        """将策略信号转换为订单提交到撮合引擎"""
        size_pct = float(self._config.position_size_pct)

        if signal == "LONG" and position_side != "LONG":
            # 如果有空仓先平仓
            if position_side == "SHORT":
                order = self._make_order(OrderSide.BUY, OrderType.MARKET, close_price, capital)
                self._matcher.submit_order(order, bar_index)
            # 开多
            qty = (capital * size_pct) / close_price
            order = self._make_order(OrderSide.BUY, OrderType.MARKET, close_price, capital, qty)
            self._matcher.submit_order(order, bar_index)

        elif signal == "SHORT" and position_side != "SHORT":
            # 如果有多仓先平仓
            if position_side == "LONG":
                order = self._make_order(OrderSide.SELL, OrderType.MARKET, close_price, capital)
                self._matcher.submit_order(order, bar_index)
            # 开空
            qty = (capital * size_pct) / close_price
            order = self._make_order(OrderSide.SELL, OrderType.MARKET, close_price, capital, qty)
            self._matcher.submit_order(order, bar_index)

        elif signal == "CLOSE" and position_side is not None:
            side = OrderSide.SELL if position_side == "LONG" else OrderSide.BUY
            order = self._make_order(side, OrderType.MARKET, close_price, capital)
            self._matcher.submit_order(order, bar_index)

    def _make_order(
        self,
        side: OrderSide,
        order_type: OrderType,
        price: float,
        capital: float,
        quantity: Optional[float] = None,
    ) -> Order:
        """创建订单"""
        if quantity is None:
            # 默认使用全部持仓或资金
            qty = (capital * float(self._config.position_size_pct)) / price if price > 0 else 0
        else:
            qty = quantity

        return Order(
            order_id=str(uuid.uuid4()),
            client_order_id=f"BT-{uuid.uuid4().hex[:8]}",
            symbol=self._config.symbol,
            exchange=self._config.exchange,
            side=side,
            order_type=order_type,
            quantity=Decimal(str(round(qty, 8))),
            price=Decimal(str(price)) if order_type == OrderType.LIMIT else None,
            status=OrderStatus.PENDING,
            strategy_id="backtest",
            created_at=int(time.time() * 1000),
            updated_at=int(time.time() * 1000),
        )

    def _row_to_kline(self, data: pl.DataFrame, idx: int) -> Kline:
        """将 DataFrame 行转换为 Kline"""
        row = data.slice(idx, 1)
        return Kline(
            symbol=self._config.symbol,
            exchange=self._config.exchange,
            timeframe=self._config.timeframe,
            timestamp=int(row["timestamp"][0]),
            open=Decimal(str(row["open"][0])),
            high=Decimal(str(row["high"][0])),
            low=Decimal(str(row["low"][0])),
            close=Decimal(str(row["close"][0])),
            volume=Decimal(str(row["volume"][0])),
            quote_volume=Decimal(str(row["volume"][0])) * Decimal(str(row["close"][0])),
            trade_count=0,
            is_closed=True,
        )

    def _create_slippage(self) -> SlippageModel:
        from core.backtest_engine.slippage import create_slippage_model
        return create_slippage_model(self._config.slippage_model, **self._config.slippage_params)

    def _create_fee(self) -> FeeModel:
        from core.backtest_engine.fee import create_fee_model
        return create_fee_model(self._config.fee_model, **self._config.fee_params)
