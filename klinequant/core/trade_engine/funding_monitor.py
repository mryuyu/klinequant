"""FundingRateMonitor — 资金费率监控

功能：
    - 定时轮询资金费率（REST）
    - 计算资金费成本/收入
    - 资金费率异常告警
    - 历史费率统计（均值/趋势）

Binance 资金费率每 8 小时结算一次（00:00, 08:00, 16:00 UTC）。
"""
from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from protocol.types import FundingRate, FundingFee, FuturesPosition

logger = logging.getLogger(__name__)


class FundingRateMonitor:
    """资金费率监控器"""

    # 默认轮询间隔（秒）
    DEFAULT_POLL_INTERVAL = 60

    # 告警阈值
    HIGH_RATE_THRESHOLD = Decimal("0.001")   # 0.1% 以上告警
    EXTREME_RATE_THRESHOLD = Decimal("0.003")  # 0.3% 以上严重告警

    def __init__(
        self,
        fetch_fn: Optional[Callable] = None,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ):
        """
        Args:
            fetch_fn: 异步函数，接收 symbol 返回 FundingRate
            poll_interval: 轮询间隔（秒）
        """
        self._fetch_fn = fetch_fn
        self._poll_interval = poll_interval
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # 当前费率: symbol → FundingRate
        self._current_rates: Dict[str, FundingRate] = {}
        # 历史费率: symbol → List[FundingRate]
        self._history: Dict[str, List[FundingRate]] = {}
        # 结算记录
        self._fee_records: List[FundingFee] = []
        # 告警回调
        self._alert_callbacks: List[Callable] = []

    @property
    def current_rates(self) -> Dict[str, FundingRate]:
        return dict(self._current_rates)

    @property
    def fee_records(self) -> List[FundingFee]:
        return list(self._fee_records)

    # ─── 生命周期 ───

    async def start(self, symbols: List[str]) -> None:
        """启动监控"""
        self._running = True
        self._symbols = symbols
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(f"FundingRateMonitor started: {symbols}")

    async def stop(self) -> None:
        """停止监控"""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("FundingRateMonitor stopped")

    async def _poll_loop(self) -> None:
        """轮询循环"""
        while self._running:
            try:
                for symbol in self._symbols:
                    if self._fetch_fn:
                        rate = await self._fetch_fn(symbol)
                        self._update_rate(symbol, rate)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Funding rate poll error: {e}")

            await asyncio.sleep(self._poll_interval)

    def _update_rate(self, symbol: str, rate: FundingRate) -> None:
        """更新费率并检查告警"""
        old_rate = self._current_rates.get(symbol)
        self._current_rates[symbol] = rate

        # 记录历史（最多保留 1000 条）
        history = self._history.setdefault(symbol, [])
        history.append(rate)
        if len(history) > 1000:
            history.pop(0)

        # 告警检查
        abs_rate = abs(rate.funding_rate)
        if abs_rate >= self.EXTREME_RATE_THRESHOLD:
            self._fire_alert(symbol, rate, "EXTREME")
        elif abs_rate >= self.HIGH_RATE_THRESHOLD:
            self._fire_alert(symbol, rate, "HIGH")

    def _fire_alert(self, symbol: str, rate: FundingRate, level: str) -> None:
        """触发告警"""
        alert = {
            "type": "FUNDING_RATE_ALERT",
            "symbol": symbol,
            "level": level,
            "rate": str(rate.funding_rate),
            "rate_percent": str(rate.rate_percent),
            "mark_price": str(rate.mark_price),
            "timestamp": rate.timestamp,
        }
        logger.warning(
            f"Funding rate alert [{level}]: {symbol} = {rate.rate_percent}%"
        )
        for cb in self._alert_callbacks:
            try:
                cb(alert)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")

    # ─── 计算 ───

    def calculate_funding_fee(
        self,
        symbol: str,
        position_qty: Decimal,
        rate: Optional[Decimal] = None,
    ) -> Decimal:
        """计算资金费

        资金费 = 持仓名义价值 × 费率
        正费率：多头支付，空头收取
        负费率：空头支付，多头收取

        Args:
            symbol: 交易对
            position_qty: 持仓量（正=多头，负=空头）
            rate: 费率（None 则使用当前费率）

        Returns:
            资金费金额（正=收入，负=支出）
        """
        if rate is None:
            current = self._current_rates.get(symbol)
            if current is None:
                return Decimal("0")
            rate = current.funding_rate

        # 多头持仓 + 正费率 = 支出（负值）
        # 空头持仓 + 正费率 = 收入（正值）
        fee = -(position_qty * rate)
        return fee

    def estimate_daily_cost(
        self,
        symbol: str,
        position_qty: Decimal,
        mark_price: Optional[Decimal] = None,
    ) -> Decimal:
        """估算每日资金费成本

        每日结算 3 次（8h 一次）
        """
        current = self._current_rates.get(symbol)
        if current is None:
            return Decimal("0")

        if mark_price is None:
            mark_price = current.mark_price

        notional = abs(position_qty) * mark_price
        per_period = notional * abs(current.funding_rate)
        return per_period * Decimal("3")  # 每天 3 次

    def get_rate_statistics(self, symbol: str) -> Dict[str, Any]:
        """获取费率统计

        Returns:
            {mean, max, min, trend, count}
        """
        history = self._history.get(symbol, [])
        if not history:
            return {"mean": 0, "max": 0, "min": 0, "trend": "FLAT", "count": 0}

        rates = [h.funding_rate for h in history]
        mean_rate = sum(rates) / len(rates)
        max_rate = max(rates)
        min_rate = min(rates)

        # 趋势：最近 10 条 vs 之前
        if len(rates) >= 20:
            recent = sum(rates[-10:]) / 10
            earlier = sum(rates[-20:-10]) / 10
            if recent > earlier * Decimal("1.2"):
                trend = "RISING"
            elif recent < earlier * Decimal("0.8"):
                trend = "FALLING"
            else:
                trend = "FLAT"
        else:
            trend = "UNKNOWN"

        return {
            "mean": mean_rate,
            "max": max_rate,
            "min": min_rate,
            "trend": trend,
            "count": len(rates),
        }

    def record_funding_fee(
        self,
        symbol: str,
        position_qty: Decimal,
        rate: Decimal,
        funding_time: int,
        strategy_id: str = "",
    ) -> FundingFee:
        """记录一次资金费结算"""
        fee_amount = self.calculate_funding_fee(symbol, position_qty, rate)
        record = FundingFee(
            symbol=symbol,
            exchange="binance_futures",
            funding_rate=rate,
            position_qty=position_qty,
            fee_amount=fee_amount,
            funding_time=funding_time,
            strategy_id=strategy_id,
        )
        self._fee_records.append(record)
        logger.info(
            f"Funding fee settled: {symbol} qty={position_qty} "
            f"rate={rate} fee={fee_amount}"
        )
        return record

    # ─── 回调注册 ───

    def add_alert_callback(self, callback: Callable) -> None:
        """注册告警回调"""
        self._alert_callbacks.append(callback)

    def update_rate_manually(self, symbol: str, rate: FundingRate) -> None:
        """手动更新费率（用于 WS 推送）"""
        self._update_rate(symbol, rate)
