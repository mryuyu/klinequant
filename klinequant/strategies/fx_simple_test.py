"""最简测试策略 — MACD + EMA 多空切换（收盘价模型）

规则（均基于已收盘 K 线的收盘价）：
  做多条件：MACD(DIF) > 0 且 close > EMA(close, 10)
  做空条件：上述任一不满足

驱动模型：收盘价模型——仅当新 bar 出现（上一根已收盘）时，
  用已收盘序列（剔除仍在形成的最后一根）算指标、判条件、开平仓；
  盘中价格波动不重算不触发，避免未收盘 bar 反复变化导致信号拖动。

反手逻辑（框架不自动跨零，策略显式两笔）：
  持多 → 条件不满足 → 先平多，再开空
  持空 → 条件满足   → 先平空，再开多

用途：验证订单系统框架闭环（send_order → Resolver → MT5 → Ledger → position）
"""
from __future__ import annotations

from decimal import Decimal

from protocol.types import Offset, OrderKind, OrderSide
from strategy.sdk.api import KqApi

# 策略参数
EMA_PERIOD = 10
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
TRADE_QTY = Decimal("0.01")  # 最小手数


def strategy(api: KqApi):
    """策略主函数"""
    api.log(f"=== Simple MACD+EMA Strategy ===")
    api.log(f"Symbol={api._symbol} Period={api._period} Qty={TRADE_QTY}")
    info = api.symbol_info()
    api.log(f"Spec: pip={info.pip_size} step={info.qty_step} min={info.min_qty}")

    last_signal = None  # "long" / "short" / None
    last_bar_ts = None  # 已处理的最新 bar 时间戳（收盘价模型：仅新 bar 出现时评估）

    while api.wait_update(deadline=5.0):
        bars = api.klines(count=200)
        if not bars or len(bars) < MACD_SLOW + MACD_SIGNAL + 6:
            continue

        # 收盘价模型：仅在出现新 bar（上一根已收盘）时评估，盘中同一根未收盘 bar 不动作
        cur_ts = bars[-1]["timestamp"]
        if last_bar_ts is not None and cur_ts == last_bar_ts:
            continue
        last_bar_ts = cur_ts

        # 剔除仍在形成的最后一根，用已收盘序列计算（确认值 = 刚收盘那根）
        closed = bars[:-1]
        if len(closed) < MACD_SLOW + MACD_SIGNAL + 5:
            continue

        closes = [Decimal(str(b["close"])) for b in closed]

        # 计算指标（基于收盘价）
        dif = _macd_dif(closes, MACD_FAST, MACD_SLOW)
        ema10 = _ema_last(closes, EMA_PERIOD)
        current_close = closes[-1]

        if dif is None or ema10 is None:
            continue

        # 信号判断
        want_long = (dif > 0) and (current_close > ema10)
        signal = "long" if want_long else "short"

        # 信号未变 → 不操作
        if signal == last_signal:
            continue

        pos = api.position()

        api.log(
            f"Signal change: {last_signal} -> {signal} | "
            f"DIF={dif:.5f} EMA10={ema10:.5f} close={current_close:.5f} | "
            f"pos vol={pos.volume} eff={pos.effective}"
        )

        # 执行反手（市价单，按市场实际规则即时成交）
        # 平仓量用 volume（=可平量），开仓判断用 effective（含在途，防重复）
        if signal == "long":
            if pos.volume < 0:  # 先平空
                r = api.send_order(OrderSide.BUY, Offset.CLOSE, abs(pos.volume),
                                   kind=OrderKind.MARKET)
                api.log(f"  CLOSE SHORT: ok={r.ok} qty={r.filled_qty} "
                        f"price={r.filled_price} reason={r.reason}")
            if pos.effective <= 0:  # 再开多
                r = api.send_order(OrderSide.BUY, Offset.OPEN, TRADE_QTY,
                                   kind=OrderKind.MARKET)
                api.log(f"  OPEN LONG: ok={r.ok} qty={r.filled_qty} "
                        f"price={r.filled_price} reason={r.reason}")
        else:  # signal == "short"
            if pos.volume > 0:  # 先平多
                r = api.send_order(OrderSide.SELL, Offset.CLOSE, pos.volume,
                                   kind=OrderKind.MARKET)
                api.log(f"  CLOSE LONG: ok={r.ok} qty={r.filled_qty} "
                        f"price={r.filled_price} reason={r.reason}")
            if pos.effective >= 0:  # 再开空
                r = api.send_order(OrderSide.SELL, Offset.OPEN, TRADE_QTY,
                                   kind=OrderKind.MARKET)
                api.log(f"  OPEN SHORT: ok={r.ok} qty={r.filled_qty} "
                        f"price={r.filled_price} reason={r.reason}")

        last_signal = signal

    api.log("Strategy exited")


# ─── 指标计算（纯函数，策略内联）───

def _ema_series(values: list[Decimal], period: int) -> list[Decimal]:
    """完整 EMA 序列"""
    if len(values) < period:
        return []
    k = Decimal("2") / (Decimal(str(period)) + Decimal("1"))
    result = [Decimal("0")] * len(values)
    sma = sum(values[:period]) / Decimal(str(period))
    result[period - 1] = sma
    for i in range(period, len(values)):
        result[i] = (values[i] - result[i - 1]) * k + result[i - 1]
    for i in range(period - 1):
        result[i] = sma
    return result


def _ema_last(values: list[Decimal], period: int) -> Decimal | None:
    """最新一个 EMA 值"""
    series = _ema_series(values, period)
    return series[-1] if series else None


def _macd_dif(values: list[Decimal], fast: int, slow: int) -> Decimal | None:
    """MACD DIF 线（快 EMA - 慢 EMA）的最新值"""
    ema_fast = _ema_series(values, fast)
    ema_slow = _ema_series(values, slow)
    if not ema_fast or not ema_slow:
        return None
    return ema_fast[-1] - ema_slow[-1]
