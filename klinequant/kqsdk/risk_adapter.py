"""RiskEngine 接线适配层（SDK 轨 → core/risk_engine）

RiskEngine 与既有 12 条规则**零改动**；本模块把 SDK 下单意图与全局状态适配成
RiskEngine 消费的 RiskContext，补三个缺口：

  1. 订单模型适配（SdkRiskAdapter）：risk_check 入参 → protocol.types.Order 壳
     （order_id=""、strategy_id=tag、市价单 price=None + extra["last_price"]）；
  2. 全局状态供给（RiskDataProvider）：RiskContext 的 daily_pnl / strategy_pnl /
     strategy_consecutive_losses / positions 由调用方喂（引擎只消费不生产）——
     共享 ExposureLedger 全 tag 聚合 = 全局统一持仓视图；日盈亏 = UTC 日起始
     权益快照差；策略盈亏 = 按 tag 轮询 realized_pnl 增量；连续亏损 = realized_pnl
     负向增量步数近似逐笔归因（决策点采样，收盘价模型下够用）；
  3. 线程安全：多策略线程共享一个 RiskEngine 实例，check 调用侧一把锁串行化
     （OrderFrequencyRule 内部 deque 有状态非线程安全；check_order 含统计自增）。

装配（assemble_risk）：runner（Live/Backtest from_config）按 YAML risk 段构造
create_default_rules + SDK 新增规则；**未在 yaml 出现的既有规则默认禁用**
（FX 语义不适用规则按默认参数会误拒）。
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from config.strategy_config import RiskConfig
from core.risk_engine.engine import RiskEngine
from core.risk_engine.rules.base import RiskCheckResult, RiskContext
from core.risk_engine.rules.rules import create_default_rules
from core.trade_engine.ledger import ExposureLedger
from kqsdk.risk_rules import create_sdk_rules
from protocol.types import (
    Account,
    Offset,
    Order,
    OrderKind,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
)

logger = logging.getLogger(__name__)

__all__ = ["RiskDataProvider", "SdkRiskAdapter", "build_risk_rules", "assemble_risk"]

# 既有 12 条规则名（create_default_rules 全集）；yaml 未列出的装配时禁用
_LEGACY_RULE_NAMES = (
    "max_order_amount", "max_position_per_symbol", "max_total_position",
    "max_daily_loss", "max_strategy_loss", "order_frequency", "price_deviation",
    "min_order_quantity", "available_balance", "consecutive_loss",
    "night_trading", "new_symbol",
)


def _utc_day(ts: float) -> str:
    """Unix 秒 → UTC 日键（跨日重置快照用）。"""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


class RiskDataProvider:
    """RiskContext 全局状态供给器（多策略线程共享，内部加锁）。

    Args:
        ledger: 共享敞口账本（全 tag = 全局统一视图）
        equity_fn: 当前账户权益（账户币；= balance + unrealized），由 runner 提供
                   （实盘/回测 executor.query_account 投影）
        symbols: 关注的品种全集（持仓视图键域；ledger 中出现的其他品种也会纳入）
        clock: 时间源（Unix 秒；单测注入假时钟验证跨日重置）
    """

    def __init__(
        self,
        ledger: ExposureLedger,
        equity_fn: Callable[[], Decimal],
        symbols: Optional[List[str]] = None,
        *,
        clock: Callable[[], float] = time.time,
    ):
        self._ledger = ledger
        self._equity_fn = equity_fn
        self._symbols = list(symbols or [])
        self._clock = clock
        self._lock = threading.Lock()
        # 日盈亏：UTC 日起始权益快照
        self._day_key: str = ""
        self._day_start_equity: Optional[Decimal] = None
        # 策略盈亏：tag → (累计 realized_pnl, 日起始基线)
        self._realized_base: Dict[str, Decimal] = {}
        # 连续亏损：tag → (上次采样累计 realized_pnl, 连续负增量步数)
        self._loss_track: Dict[str, tuple[Decimal, int]] = {}

    # ─── 采样（每次 risk_check 决策点调用）───

    def _roll_day_locked(self, day: str) -> None:
        """UTC 跨日：重置权益快照与策略盈亏基线（调用方持锁）。"""
        if day == self._day_key:
            return
        self._day_key = day
        try:
            self._day_start_equity = self._equity_fn()
        except Exception as e:
            logger.warning(f"[RISK] equity_fn failed on day roll: {e}")
            self._day_start_equity = None
        for tag in self._realized_base:
            self._realized_base[tag] = self._tag_realized(tag)

    def _tag_realized(self, tag: str) -> Decimal:
        """某策略（tag 前缀）当前累计 realized_pnl（全品种求和）。"""
        total = Decimal("0")
        for pos in self._ledger.all_positions():
            if _base_tag(pos.tag) == tag:
                total += pos.realized_pnl
        # all_positions 过滤掉空仓 tag，realized_pnl 仍可能挂在已平 tag 上——
        # 决策点采样口径下可接受（平仓即清零基线，日内增量不受影响）
        return total

    def snapshot(
        self, *, tag: str, offset: Offset, last_price: Optional[Decimal] = None,
    ) -> Dict[str, Any]:
        """采集全局状态 → RiskContext 字段包（dict，adapter 组装用）。"""
        now = self._clock()
        with self._lock:
            day = _utc_day(now)
            self._roll_day_locked(day)

            # 日盈亏（权益差）+ 百分比（占日起始权益）
            equity: Optional[Decimal] = None
            try:
                equity = self._equity_fn()
            except Exception as e:
                logger.warning(f"[RISK] equity_fn failed: {e}")
            daily_pnl = Decimal("0")
            daily_pnl_pct = Decimal("0")
            if equity is not None and self._day_start_equity is not None:
                daily_pnl = equity - self._day_start_equity
                if self._day_start_equity != 0:
                    daily_pnl_pct = (
                        daily_pnl / abs(self._day_start_equity) * Decimal("100")
                    )

            # 策略盈亏（按 tag 前缀的 realized_pnl 日内增量）
            base_tag = _base_tag(tag)
            strategy_pnl: Dict[str, Decimal] = {}
            losses: Dict[str, int] = {}
            for t in self._known_tags():
                cur = self._tag_realized(t)
                base = self._realized_base.get(t)
                if base is None:
                    self._realized_base[t] = cur
                    base = cur
                strategy_pnl[t] = cur - base
                # 连续亏损计数：负向增量步进 +1，正向增量清零（近似逐笔归因）
                prev, cnt = self._loss_track.get(t, (cur, 0))
                delta = cur - prev
                if delta < 0:
                    cnt += 1
                elif delta > 0:
                    cnt = 0
                self._loss_track[t] = (cur, cnt)
                losses[t] = cnt

            # 全局统一持仓视图：全 tag 按 symbol 聚合净持仓
            positions = self._positions_view()
            total_positions, strategy_positions = self._position_counts()

            extra: Dict[str, Any] = {
                "daily_pnl_pct": daily_pnl_pct,
                "total_positions": total_positions,
                "strategy_positions": strategy_positions,
                "offset": offset.value,
            }
            if last_price is not None:
                extra["last_price"] = last_price
            return {
                "daily_pnl": daily_pnl,
                "strategy_pnl": {base_tag: strategy_pnl.get(base_tag, Decimal("0"))},
                "strategy_consecutive_losses": {
                    base_tag: losses.get(base_tag, 0)
                },
                "positions": positions,
                "extra": extra,
                "timestamp": int(now * 1000),
            }

    def _known_tags(self) -> List[str]:
        """账本中出现过的全部策略基名（tag 前缀）。"""
        tags = set()
        for pos in self._ledger.all_positions():
            if pos.tag:
                tags.add(_base_tag(pos.tag))
        return sorted(tags)

    def _positions_view(self) -> Dict[str, Position]:
        """全 tag 聚合为 {symbol: Position}（quantity=带符号净手数，全局统一视图）。"""
        agg: Dict[str, Position] = {}
        symbols = set(self._symbols)
        for pos in self._ledger.all_positions():
            symbols.add(pos.symbol)
        for sym in symbols:
            volume = Decimal("0")
            avg = Decimal("0")
            for pos in self._ledger.all_positions():
                if pos.symbol == sym:
                    volume += pos.effective
                    if pos.avg_entry_price:
                        avg = pos.avg_entry_price
            agg[sym] = Position(
                symbol=sym,
                side="LONG" if volume > 0 else ("SHORT" if volume < 0 else "FLAT"),
                quantity=volume,
                volume=volume,
                avg_entry_price=avg,
            )
        return agg

    def _position_counts(self) -> tuple[int, Dict[str, int]]:
        """(全局持仓笔数, {策略基名: 笔数})；笔数 = effective≠0 的 (symbol,tag)。"""
        total = 0
        per: Dict[str, int] = {}
        for pos in self._ledger.all_positions():
            if pos.effective == 0:
                continue
            total += 1
            bt = _base_tag(pos.tag)
            per[bt] = per.get(bt, 0) + 1
        return total, per


class SdkRiskAdapter:
    """SDK 下单意图 → RiskEngine.check_order 适配（共享实例，调用侧加锁）。"""

    def __init__(self, engine: RiskEngine, provider: RiskDataProvider):
        self._engine = engine
        self._provider = provider
        self._lock = threading.Lock()

    @property
    def engine(self) -> RiskEngine:
        return self._engine

    def check(
        self,
        *,
        symbol: str,
        tag: str,
        side: OrderSide,
        offset: Offset,
        qty: Decimal,
        kind: OrderKind = OrderKind.MARKET,
        price: Optional[Decimal] = None,
        last_price: Optional[Decimal] = None,
        account: Optional[Account] = None,
    ) -> RiskCheckResult:
        """风控预检（fail-closed 由 RiskEngine 保证）。

        市价单 price=None → 规则用 extra["last_price"]（feed 最新价）估算名义值。
        """
        order = Order(
            order_id="",
            symbol=symbol,
            exchange="",
            side=side,
            order_type=(
                OrderType.MARKET if kind == OrderKind.MARKET else OrderType.LIMIT
            ),
            quantity=qty,
            status=OrderStatus.PENDING,
            strategy_id=tag,
            price=price,
        )
        snap = self._provider.snapshot(
            tag=tag, offset=offset,
            last_price=last_price if price is None else None,
        )
        ctx = RiskContext(
            order=order,
            account=account,
            positions=snap["positions"],
            daily_pnl=snap["daily_pnl"],
            strategy_pnl=snap["strategy_pnl"],
            strategy_consecutive_losses=snap["strategy_consecutive_losses"],
            timestamp=snap["timestamp"],
            extra=snap["extra"],
        )
        with self._lock:   # 共享实例串行化（规则内部状态 + 统计自增非线程安全）
            return self._engine.check_order(ctx)


def _base_tag(tag: str) -> str:
    """tag(``{strategy}`` 或 ``{strategy}:{period}``) → 策略基名。"""
    return tag.split(":", 1)[0] if tag else tag


# ─── 装配 ───


def build_risk_rules(cfg: RiskConfig) -> List[Any]:
    """按 YAML risk 段构造规则列表。

    既有 12 条：yaml 列出 → enabled 按配置（缺省 True）+ 参数更新；未列出 →
    禁用（避免 FX 不适用规则按默认参数误拒）。SDK 新增 3 条：参数 >0 即启用。
    """
    legacy: List[Any] = create_default_rules()
    for rule in legacy:
        rc: Optional[Dict[str, Any]] = cfg.rules.get(rule.name)
        if rc is None:
            rule.enabled = False
            continue
        # 全量透传（含 enabled）：NightTradingRule 等规则内部也读 enabled 参数；
        #   其余规则忽略多余键。rule.enabled 单独按配置设置（未在 yaml=False）。
        rule.update_params(dict(rc))
        rule.enabled = bool(rc.get("enabled", True))

    sdk = create_sdk_rules(
        max_daily_loss_pct=cfg.max_daily_loss_pct,
        max_total_positions=cfg.max_total_positions,
        max_positions_per_strategy=cfg.max_positions_per_strategy,
    )

    known = set(_LEGACY_RULE_NAMES) | {r.name for r in sdk}
    unknown = [n for n in cfg.rules if n not in known]
    if unknown:
        logger.warning(f"[RISK] yaml 中未知规则名（忽略）：{unknown}")
    return legacy + sdk


def assemble_risk(
    cfg: RiskConfig,
    ledger: ExposureLedger,
    equity_fn: Callable[[], Decimal],
    symbols: Optional[List[str]] = None,
) -> SdkRiskAdapter:
    """构造完整风控组件（runner from_config 调用）：规则装配 + engine.start()。

    风控拒绝/异常经 add_log_callback 落日志（带策略名前缀，见 _risk_log）。
    """
    rules = build_risk_rules(cfg)
    engine = RiskEngine(rules=rules, fail_closed=True)
    engine.add_log_callback(_risk_log)
    engine.start()
    enabled = [r.name for r in rules if r.enabled]
    logger.info(f"[RISK] RiskEngine assembled, enabled rules: {enabled}")
    provider = RiskDataProvider(ledger, equity_fn, symbols)
    return SdkRiskAdapter(engine, provider)


def _risk_log(entry: Dict[str, Any]) -> None:
    """RiskEngine 日志回调 → 策略日志（带策略名前缀）。"""
    logger.warning(
        f"[RISK-REJECT:{entry.get('strategy_id', '')}:{entry.get('symbol', '')}] "
        f"{entry.get('rule_name', '')} [{entry.get('level', '')}] {entry.get('message', '')}"
    )
