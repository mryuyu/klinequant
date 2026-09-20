"""FX MACD Demo 策略 — 验证订单系统闭环

逻辑：
  - 用 1m K线计算简易 EMA 交叉（快线 12、慢线 26）
  - 金叉（快上穿慢）且无持仓 → 买开 0.01 lot
  - 死叉（快下穿慢）且持多 → 卖平
  - 仅用于验证框架闭环，非盈利策略

运行：
    python scripts/run_fx_live.py
"""
from __future__ import annotations

from decimal import Decimal

from protocol.types import Offset, OrderKind, OrderSide
from strategy.sdk.api import KqApi


def strategy(api: KqApi):
    """策略主函数。由 LiveRunner 调用，内部 while wait_update 循环。"""

    # 策略参数
    fast_period = 12
    slow_period = 26
    trade_qty = Decimal("0.01")  # 最小手数

    api.log(f"Strategy started: {api._symbol}/{api._period} tag={api._tag}")
    api.log(f"SymbolInfo: step={api.symbol_info().qty_step} min={api.symbol_info().min_qty} pip={api.symbol_info().pip_size}")

    # 等待足够的 K 线数据
    bars_needed = slow_period + 5
    bar_index = 0

    while api.wait_update(deadline=5.0):
        # 获取 K 线
        bars = api.klines(count=200)
        if not bars or len(bars) < bars_needed:
            continue

        # 只在 bar 收盘时计算（最后一根 bar 的 timestamp 变化）
        if not api.is_changing(bars):
            continue

        bar_index += 1

        # 计算 EMA
        closes = [Decimal(str(b["close"])) for b in bars]
        ema_fast = _ema(closes, fast_period)
        ema_slow = _ema(closes, slow_period)

        if ema_fast is None or ema_slow is None:
            continue

        # 当前和前一根的 EMA 值
        fast_now = ema_fast[-1]
        slow_now = ema_slow[-1]
        fast_prev = ema_fast[-2]
        slow_prev = ema_slow[-2]

        # 交叉检测
        golden_cross = fast_prev <= slow_prev and fast_now > slow_now
        death_cross = fast_prev >= slow_prev and fast_now < slow_now

        # 读取持仓
        pos = api.position()
        tick = api.ticks()

        if tick is None:
            continue

        # 金叉开多
        if golden_cross and pos.effective == 0:
            api.log(f"Golden cross detected: fast={fast_now:.5f} > slow={slow_now:.5f}")
            result = api.send_order(
                OrderSide.BUY, Offset.OPEN, trade_qty,
                kind=OrderKind.LIMIT, price=tick.ask_price,
            )
            if result.ok:
                api.log(f"OPEN LONG ok: qty={result.filled_qty} price={result.filled_price}")
            else:
                api.log(f"OPEN LONG rejected: {result.reason}")

        # 死叉平多
        elif death_cross and pos.effective > 0:
            api.log(f"Death cross detected: fast={fast_now:.5f} < slow={slow_now:.5f}")
            result = api.send_order(
                OrderSide.SELL, Offset.CLOSE, pos.effective,
                kind=OrderKind.LIMIT, price=tick.bid_price,
            )
            if result.ok:
                api.log(f"CLOSE LONG ok: qty={result.filled_qty} price={result.filled_price}")
            else:
                api.log(f"CLOSE LONG rejected: {result.reason}")

        # 每 60 根 bar 打印一次状态
        if bar_index % 60 == 0:
            api.log(
                f"[STATUS] bar={bar_index} pos={pos.effective} "
                f"fast={fast_now:.5f} slow={slow_now:.5f} "
                f"bid={tick.bid_price} ask={tick.ask_price}"
            )

    api.log("Strategy loop exited (wait_update returned False)")


def _ema(values: list[Decimal], period: int) -> list[Decimal] | None:
    """计算 EMA 序列（返回与 values 等长的列表，前 period-1 个为 None 用首值填充）"""
    if len(values) < period:
        return None

    multiplier = Decimal("2") / (Decimal(str(period)) + Decimal("1"))
    result = [Decimal("0")] * len(values)

    # 前 period 个用 SMA 初始化
    sma = sum(values[:period]) / Decimal(str(period))
    result[period - 1] = sma

    # 递推
    for i in range(period, len(values)):
        result[i] = (values[i] - result[i - 1]) * multiplier + result[i - 1]

    # 前 period-1 个填充初始值（避免 None 判断）
    for i in range(period - 1):
        result[i] = sma

    return result
