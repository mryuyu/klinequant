"""Performance — 绩效分析

15 项核心绩效指标：
    1. total_return: 总收益率
    2. annual_return: 年化收益率
    3. max_drawdown: 最大回撤
    4. max_drawdown_duration: 最大回撤持续时间（bars）
    5. sharpe_ratio: 夏普比率
    6. sortino_ratio: 索提诺比率
    7. calmar_ratio: 卡尔玛比率
    8. win_rate: 胜率
    9. profit_factor: 盈亏比
    10. avg_win: 平均盈利
    11. avg_loss: 平均亏损
    12. max_consecutive_wins: 最大连续盈利次数
    13. max_consecutive_losses: 最大连续亏损次数
    14. total_trades: 总交易次数
    15. total_fees: 总手续费

遵循需求文档 §4.5 BT-004。
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional


@dataclass
class Trade:
    """单笔交易（一开一平）"""

    symbol: str
    side: str  # LONG / SHORT
    entry_price: Decimal
    exit_price: Decimal
    quantity: Decimal
    entry_time: int
    exit_time: int
    pnl: Decimal  # 净盈亏（扣费后）
    fee: Decimal
    bars_held: int = 0


@dataclass
class PerformanceReport:
    """绩效报告"""

    total_return: float = 0.0
    annual_return: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_duration: int = 0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    total_trades: int = 0
    total_fees: float = 0.0

    # 附加信息
    initial_capital: float = 0.0
    final_equity: float = 0.0
    equity_curve: List[float] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_return": self.total_return,
            "annual_return": self.annual_return,
            "max_drawdown": self.max_drawdown,
            "max_drawdown_duration": self.max_drawdown_duration,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "calmar_ratio": self.calmar_ratio,
            "win_rate": self.win_rate,
            "profit_factor": self.profit_factor,
            "avg_win": self.avg_win,
            "avg_loss": self.avg_loss,
            "max_consecutive_wins": self.max_consecutive_wins,
            "max_consecutive_losses": self.max_consecutive_losses,
            "total_trades": self.total_trades,
            "total_fees": self.total_fees,
            "initial_capital": self.initial_capital,
            "final_equity": self.final_equity,
        }


class PerformanceAnalyzer:
    """绩效分析器

    输入：
        - equity_curve: 每根 K 线的权益曲线
        - trades: 已完成交易列表
        - initial_capital: 初始资金
        - bars_per_year: 年化换算（如 1m = 525600, 1h = 8760, 1d = 365）
    """

    def __init__(self, bars_per_year: int = 525600):
        self._bars_per_year = bars_per_year

    def analyze(
        self,
        equity_curve: List[float],
        trades: List[Trade],
        initial_capital: float,
    ) -> PerformanceReport:
        """计算全部 15 项绩效指标"""
        report = PerformanceReport(
            initial_capital=initial_capital,
            equity_curve=equity_curve,
        )

        if not equity_curve:
            return report

        final_equity = equity_curve[-1]
        report.final_equity = final_equity

        # 1. 总收益率
        report.total_return = (final_equity - initial_capital) / initial_capital

        # 2. 年化收益率
        n_bars = len(equity_curve)
        if n_bars > 1:
            years = n_bars / self._bars_per_year
            if years > 0 and final_equity > 0 and initial_capital > 0:
                try:
                    report.annual_return = (final_equity / initial_capital) ** (1 / years) - 1
                except (OverflowError, ZeroDivisionError):
                    report.annual_return = 0.0

        # 3-4. 最大回撤 + 持续时间
        report.max_drawdown, report.max_drawdown_duration = self._calc_max_drawdown(equity_curve)

        # 5. 夏普比率（假设无风险利率 = 0）
        returns = self._calc_returns(equity_curve)
        if returns:
            report.sharpe_ratio = self._calc_sharpe(returns)
            report.sortino_ratio = self._calc_sortino(returns)

        # 7. 卡尔玛比率
        if report.max_drawdown > 0:
            report.calmar_ratio = report.annual_return / report.max_drawdown

        # 8-15. 基于交易的指标
        if trades:
            report.total_trades = len(trades)
            report.total_fees = sum(float(t.fee) for t in trades)

            wins = [t for t in trades if t.pnl > 0]
            losses = [t for t in trades if t.pnl <= 0]

            report.win_rate = len(wins) / len(trades)

            gross_profit = sum(float(t.pnl) for t in wins)
            gross_loss = abs(sum(float(t.pnl) for t in losses))
            report.profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

            report.avg_win = gross_profit / len(wins) if wins else 0.0
            report.avg_loss = gross_loss / len(losses) if losses else 0.0

            report.max_consecutive_wins = self._max_consecutive(trades, win=True)
            report.max_consecutive_losses = self._max_consecutive(trades, win=False)

        return report

    def _calc_max_drawdown(self, curve: List[float]) -> tuple:
        """计算最大回撤和持续时间"""
        if not curve:
            return 0.0, 0

        peak = curve[0]
        max_dd = 0.0
        max_dd_duration = 0
        current_dd_start = 0
        in_drawdown = False

        for i, val in enumerate(curve):
            if val >= peak:
                peak = val
                if in_drawdown:
                    duration = i - current_dd_start
                    max_dd_duration = max(max_dd_duration, duration)
                    in_drawdown = False
            else:
                if not in_drawdown:
                    in_drawdown = True
                    current_dd_start = i
                dd = (peak - val) / peak if peak > 0 else 0
                max_dd = max(max_dd, dd)

        # 如果最后仍在回撤中
        if in_drawdown:
            duration = len(curve) - 1 - current_dd_start
            max_dd_duration = max(max_dd_duration, duration)

        return max_dd, max_dd_duration

    def _calc_returns(self, curve: List[float]) -> List[float]:
        """计算逐 bar 收益率"""
        returns = []
        for i in range(1, len(curve)):
            if curve[i - 1] != 0:
                returns.append((curve[i] - curve[i - 1]) / curve[i - 1])
        return returns

    def _calc_sharpe(self, returns: List[float], risk_free: float = 0.0) -> float:
        """夏普比率 = (mean - rf) / std * sqrt(bars_per_year)"""
        n = len(returns)
        if n < 2:
            return 0.0
        mean = sum(returns) / n
        variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
        if std == 0:
            return 0.0
        bar_rf = risk_free / self._bars_per_year
        return (mean - bar_rf) / std * math.sqrt(self._bars_per_year)

    def _calc_sortino(self, returns: List[float], risk_free: float = 0.0) -> float:
        """索提诺比率：只考虑下行波动"""
        n = len(returns)
        if n < 2:
            return 0.0
        mean = sum(returns) / n
        bar_rf = risk_free / self._bars_per_year
        downside = [r for r in returns if r < bar_rf]
        if not downside:
            return float("inf") if mean > bar_rf else 0.0
        downside_var = sum((r - bar_rf) ** 2 for r in downside) / len(downside)
        downside_std = math.sqrt(downside_var)
        if downside_std == 0:
            return 0.0
        return (mean - bar_rf) / downside_std * math.sqrt(self._bars_per_year)

    def _max_consecutive(self, trades: List[Trade], win: bool) -> int:
        """最大连续盈利/亏损次数"""
        max_streak = 0
        current = 0
        for t in trades:
            if (t.pnl > 0) == win:
                current += 1
                max_streak = max(max_streak, current)
            else:
                current = 0
        return max_streak
