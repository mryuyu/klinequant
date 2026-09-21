"""BacktestExecutor — 回测撮合执行器（实现 ExecutorProtocol）

与 Mt5Executor 暴露同一 submit/query 契约（返回同一 SubmitResult、同形持仓/账户
dict），使 KqApi / ExposureLedger / UnifiedResolver 在回测中零改动复用（同构）。

撮合模型（一期）：
  - 仅支持 MARKET 单；LIMIT/STOP 返回 DEAD（回测 v1 不模拟挂单触发）。
  - 成交价 = 当前 bar 开盘价 ± 滑点。策略以 bars[:-1]（已收盘）算信号、下单，
    故成交落在信号 bar 的下一根开盘，天然反 look-ahead，与实盘语义一致。
  - 净持仓（Netting）记账：与 resolver close_priority="net" 及策略「先平后开」一致。
  - 盈亏按 contract_multiplier 换算：pnl = 价差 × 手数 × 合约乘数（计价货币）。
  - 手续费按「名义额 = 成交价 × 手数 × 合约乘数」计（PercentageFee）或每笔固定
    （FixedFee），逐笔从现金扣除，平仓时归属到对应 Trade。

币种换算：盈亏/手续费从计价货币换算到账户币（默认 USD）——
  quote==账户币（如 EURUSD）原样；base==账户币（如 USDJPY）÷价格；
  交叉盘（如 EURJPY）缺第三方汇率无法换算，按计价货币原样计并告警一次。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional

from core.backtest_engine.fee import create_fee_model
from core.backtest_engine.performance import Trade
from core.backtest_engine.slippage import create_slippage_model
from core.trade_engine.executors.mt5_executor import SubmitResult
from core.trade_engine.resolver import VenueOrderSpec
from gateway.market_sources.mt5_driver import TRADE_RETCODE_DONE
from protocol.types import DeadReason, OrderKind, OrderSide, SymbolInfo
from strategy.sdk.backtest_feed import BacktestDataFeed

logger = logging.getLogger(__name__)


@dataclass
class _OpenPos:
    """单品种净持仓（Netting）"""
    qty: Decimal              # 绝对量（手数）
    side: str                 # "LONG" / "SHORT"
    avg_entry: Decimal        # 加权平均开仓价
    entry_time: int           # 开仓时间（ms）
    entry_index: int          # 开仓 bar index
    acc_fee: Decimal          # 建立当前持仓累计手续费（平仓时归属到 Trade）
    ticket: int = 0


@dataclass
class BacktestExecutor:
    """回测执行器（模拟撮合 + 净持仓记账 + 资金曲线）"""

    def __post_init__(self) -> None:
        self._cash: Decimal = Decimal(str(self.initial_capital))
        self._initial: Decimal = Decimal(str(self.initial_capital))
        self._slippage = create_slippage_model(
            self.slippage_model, **(self.slippage_params or {})
        )
        self._fee = create_fee_model(self.fee_model, **(self.fee_params or {}))
        self._pos: Dict[str, _OpenPos] = {}
        self._equity_curve: List[float] = []
        self._trades: List[Trade] = []
        self._total_fees: Decimal = Decimal("0")
        self._ticket_seq: int = 1000
        self._conv_warned: set = set()
        # 注册逐 bar mark-to-market 回调
        self.feed.set_on_bar(self.mark_equity)

    # ─── 构造参数（dataclass 字段）───
    feed: BacktestDataFeed = field(default=None)          # type: ignore[assignment]
    specs: Dict[str, SymbolInfo] = field(default_factory=dict)
    initial_capital: Decimal = Decimal("10000")
    slippage_model: str = "percentage"
    slippage_params: Optional[dict] = None
    fee_model: str = "fixed"
    fee_params: Optional[dict] = None
    magic: int = 202609
    account_currency: str = "USD"

    # ─── ExecutorProtocol 实现 ───

    def submit(self, spec: VenueOrderSpec) -> SubmitResult:
        """模拟下单成交。仅 MARKET；成交价 = 当前 bar 开盘 ± 滑点。"""
        if spec.kind != OrderKind.MARKET:
            return SubmitResult(
                success=False, status="DEAD",
                dead_reason=DeadReason.REJECTED.value,
                comment=f"backtest v1 supports MARKET only, got {spec.kind.value}",
            )

        sym = spec.symbol.upper()
        info = self.specs.get(sym) or self.specs.get(spec.symbol)
        if info is None:
            return SubmitResult(
                success=False, status="DEAD",
                dead_reason=DeadReason.FAILED.value,
                comment=f"no SymbolInfo for {sym}",
            )

        base = self.feed.open_price(sym)
        if base is None:
            return SubmitResult(
                success=False, status="DEAD",
                dead_reason=DeadReason.FAILED.value,
                comment=f"no bar data for {sym} at index {self.feed.index}",
            )

        side_str = "BUY" if spec.side == OrderSide.BUY else "SELL"
        fill_price = self._slippage.calculate(
            Decimal(str(base)), spec.qty, side_str, None
        )
        # 手续费按名义额（手数 × 合约乘数）计，换算到账户币
        notional_qty = spec.qty * info.contract_multiplier
        fee_q = self._fee.calculate(fill_price, notional_qty, side_str, False)
        fee = self._conv(sym, fee_q, fill_price)

        self._total_fees += fee
        self._cash -= fee
        self._apply_fill(sym, spec.side, spec.qty, fill_price, fee, info)

        self._ticket_seq += 1
        logger.debug(
            f"[BT] {sym} {side_str}/{spec.offset.value} qty={spec.qty} "
            f"fill@{fill_price} fee={fee}"
        )
        return SubmitResult(
            success=True, status="FILLED",
            order_ticket=self._ticket_seq,
            filled_qty=spec.qty,
            filled_price=fill_price,
            retcode=TRADE_RETCODE_DONE,
            comment=f"backtest fill @{fill_price}",
        )

    def cancel(self, order_ticket: int, symbol: str) -> bool:
        """回测 v1 无挂单，撤单恒 False"""
        return False

    def query_positions(self, symbol: str = "") -> List[Dict]:
        """模拟持仓（MT5 形状 dict，供 KqApi.flatten / 对账消费）"""
        out: List[Dict] = []
        for sym, pos in self._pos.items():
            if symbol and sym != symbol.upper():
                continue
            if pos.qty <= 0:
                continue
            cur = self.feed.close_price(sym)
            cur_px = Decimal(str(cur)) if cur is not None else pos.avg_entry
            mult = self._mult(sym)
            if pos.side == "LONG":
                u_q = (cur_px - pos.avg_entry) * pos.qty * mult
            else:
                u_q = (pos.avg_entry - cur_px) * pos.qty * mult
            unreal = self._conv(sym, u_q, cur_px)
            out.append({
                "ticket": pos.ticket,
                "symbol": sym,
                "type": 0 if pos.side == "LONG" else 1,
                "volume": float(pos.qty),
                "price_open": float(pos.avg_entry),
                "price_current": float(cur_px),
                "profit": float(unreal),
                "magic": self.magic,
                "time": int(pos.entry_time // 1000),
                "comment": "backtest",
            })
        return out

    def query_account(self) -> Optional[Dict]:
        """模拟账户（MT5 形状 dict）"""
        unreal = self._unrealized()
        equity = self._cash + unreal
        return {
            "balance": float(self._cash),
            "equity": float(equity),
            "margin": 0.0,
            "margin_free": float(self._cash),
            "profit": float(unreal),
            "currency": "ACCOUNT",
        }

    def query_orders(self, symbol: str = "") -> List[Dict]:
        """回测 v1 无挂单"""
        return []

    # ─── 绩效采样 ───

    def mark_equity(self, index: Optional[int] = None) -> None:
        """逐 bar mark-to-market，追加一个资金曲线点（feed.on_bar 回调）"""
        equity = self._cash + self._unrealized()
        self._equity_curve.append(float(equity))

    @property
    def equity_curve(self) -> List[float]:
        return self._equity_curve

    @property
    def trades(self) -> List[Trade]:
        return self._trades

    @property
    def total_fees(self) -> Decimal:
        return self._total_fees

    @property
    def cash(self) -> Decimal:
        return self._cash

    # ─── 内部：净持仓记账 ───

    def _mult(self, sym: str) -> Decimal:
        info = self.specs.get(sym)
        return info.contract_multiplier if info else Decimal("1")

    def _conv(self, sym: str, amount_quote: Decimal, price: Decimal) -> Decimal:
        """计价货币金额 → 账户币（默认 USD）。

        quote==账户币（EURUSD）原样；base==账户币（USDJPY）÷价格；
        交叉盘（EURJPY）缺第三方汇率，按计价货币原样并告警一次。
        """
        info = self.specs.get(sym)
        if info is None:
            return amount_quote
        q = (info.quote_currency or "").upper()
        b = (info.base_currency or "").upper()
        acct = self.account_currency.upper()
        if q == acct:
            return amount_quote
        if b == acct and price and price > 0:
            return amount_quote / Decimal(str(price))
        if sym not in self._conv_warned:
            self._conv_warned.add(sym)
            logger.warning(
                f"[BT] {sym} 交叉盘 {b}/{q} 盈亏无法换算到 {acct}，"
                f"按计价货币 {q} 原样计（绝对值不可比）"
            )
        return amount_quote

    def _unrealized(self) -> Decimal:
        total = Decimal("0")
        for sym, pos in self._pos.items():
            if pos.qty <= 0:
                continue
            cur = self.feed.close_price(sym)
            if cur is None:
                continue
            cur_px = Decimal(str(cur))
            mult = self._mult(sym)
            if pos.side == "LONG":
                u_q = (cur_px - pos.avg_entry) * pos.qty * mult
            else:
                u_q = (pos.avg_entry - cur_px) * pos.qty * mult
            total += self._conv(sym, u_q, cur_px)
        return total

    def _apply_fill(
        self, sym: str, side: OrderSide, qty: Decimal,
        fill_price: Decimal, fee: Decimal, info: SymbolInfo,
    ) -> None:
        """更新净持仓 + 结算已实现盈亏 + 记录 Trade"""
        mult = info.contract_multiplier
        signed = qty if side == OrderSide.BUY else -qty
        pos = self._pos.get(sym)
        cur_net = Decimal("0")
        if pos is not None:
            cur_net = pos.qty if pos.side == "LONG" else -pos.qty
        new_net = cur_net + signed

        # 本次成交中的平仓量（与现有持仓反向的部分）
        closed = Decimal("0")
        if cur_net > 0 and signed < 0:
            closed = min(cur_net, -signed)
        elif cur_net < 0 and signed > 0:
            closed = min(-cur_net, signed)

        now_ms = self.feed.now_ms()
        cur_index = self.feed.index

        if closed > 0 and pos is not None:
            if pos.side == "LONG":
                gross_q = (fill_price - pos.avg_entry) * closed * mult
            else:
                gross_q = (pos.avg_entry - fill_price) * closed * mult
            gross = self._conv(sym, gross_q, fill_price)
            open_fee_portion = (
                pos.acc_fee * (closed / pos.qty) if pos.qty > 0 else Decimal("0")
            )
            trade_fee = open_fee_portion + fee
            self._cash += gross
            self._trades.append(Trade(
                symbol=sym,
                side=pos.side,
                entry_price=pos.avg_entry,
                exit_price=fill_price,
                quantity=closed,
                entry_time=pos.entry_time,
                exit_time=now_ms,
                pnl=gross - trade_fee,
                fee=trade_fee,
                bars_held=max(0, cur_index - pos.entry_index),
            ))
            pos.qty -= closed
            pos.acc_fee -= open_fee_portion

            if pos.qty <= 0:
                remainder = abs(new_net)
                if remainder > 0:
                    # 单笔跨零反手：剩余部分以本次成交价开新仓
                    self._ticket_seq += 1
                    self._pos[sym] = _OpenPos(
                        qty=remainder,
                        side="LONG" if new_net > 0 else "SHORT",
                        avg_entry=fill_price,
                        entry_time=now_ms,
                        entry_index=cur_index,
                        acc_fee=fee,
                        ticket=self._ticket_seq,
                    )
                else:
                    self._pos.pop(sym, None)
                return
            # 部分平仓，保留剩余持仓
            return

        # 纯开仓 / 同向加仓
        if pos is not None and (
            (pos.side == "LONG" and signed > 0) or (pos.side == "SHORT" and signed < 0)
        ):
            total = pos.qty + qty
            pos.avg_entry = (pos.avg_entry * pos.qty + fill_price * qty) / total
            pos.qty = total
            pos.acc_fee += fee
        else:
            self._ticket_seq += 1
            self._pos[sym] = _OpenPos(
                qty=abs(signed),
                side="LONG" if signed > 0 else "SHORT",
                avg_entry=fill_price,
                entry_time=now_ms,
                entry_index=cur_index,
                acc_fee=fee,
                ticket=self._ticket_seq,
            )
