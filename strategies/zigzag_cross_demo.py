"""条件驱动 ZigZag 转折示范策略（kqsdk · indic.zigzag() 用法示范）

演示 ZigZag 极值点/交叉事件在策略中的消费方式（回测/实盘同一套代码）::

    from klinequant.kqsdk import strategy, KqApi, StrategyContext, zigzag_high, zigzag_low

    @strategy("zigzag-cross-demo")
    def zigzag_cross(api: KqApi, ctx: StrategyContext) -> None:
        zz = api.INDIC().zigzag(bull="cross_up(ma(close,5), ema(close,20))", ...)
        h1 = zz.zigzag_high()      # 或函数形态 zigzag_high(zz)
        ...

信号模型（收盘价模型，反 look-ahead）：仅当**新 bar 出现**才评估；ZigZag 状态机
翻转确认新极值点时行动——
  - 确认新低点 L（条件金叉翻转，多头 leg 开启）→ 做多；
  - 确认新高点 H（条件死叉翻转，空头 leg 开启）→ 做空。
pending 极值点（H1/L1 未确认）**不参与**信号（允许重绘，确认后才稳定）。

参数（yaml → ctx.params）：
  bull / bear       ZigZag 多空态条件表达式（受限文法，见 core/indicator_engine/zigzag.py）
  cross_type        时间窗修正扫描交叉类型（空 = 仅条件内交叉谓词对）
  tolerance         交叉判定容差（抑制浮点抖动）
  time_window       时间窗修正根数（0 关闭）
  risk_pct          单笔风险 = 账户权益 × risk_pct%
  atr_period/atr_mult 止损距离 = ATR × atr_mult + 1 pip 点差缓冲
  rr_mult           盈亏比（tp = 入场价 ± stop_distance × rr_mult）

运行（需在 config/strategies.yaml 自行配置实例后）：
  python scripts/run_strategies.py --config config/strategies.yaml --mode backtest
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

from klinequant.kqsdk import (
    KqApi,
    Offset,
    OrderKind,
    OrderSide,
    StrategyContext,
    zigzag_high,
    zigzag_low,
)
from klinequant.kqsdk import strategy as kq_strategy


@kq_strategy("zigzag-cross-demo")
def zigzag_cross(api: KqApi, ctx: StrategyContext) -> None:
    """ZigZag 转折策略主函数（双参形态 ``strategy(api, ctx)``，由 runner 调用）。"""
    # ─── 参数解析（全部来自 yaml，不写死）───
    p = ctx.params or {}
    bull = str(p.get("bull", "cross_up(ma(close,5), ema(close,20))"))
    bear = str(p.get("bear", "cross_down(ma(close,5), ema(close,20))"))
    cross_type = str(p.get("cross_type", ""))
    tolerance = float(p.get("tolerance", 0.0))
    time_window = int(p.get("time_window", 0))
    risk_pct = float(p.get("risk_pct", 1.0))
    atr_period = int(p.get("atr_period", 14))
    atr_mult = float(p.get("atr_mult", 1.5))
    rr_mult = float(p.get("rr_mult", 2.0))

    info = api.symbol_info()
    sym = info.symbol
    api.log(
        f"=== {ctx.name} ZigZag 转折 bull={bull!r} bear={bear!r} "
        f"cross_type={cross_type!r} tol={tolerance} window={time_window} ==="
    )

    # ─── 声明指标活视图（计算下沉 IndicatorEngine，只声明一次）───
    indic = api.INDIC()
    zz = indic.zigzag(
        bull=bull, bear=bear, cross_type=cross_type,
        tolerance=tolerance, time_window=time_window,
    )
    atr_v = indic.atr(atr_period).field("ATR")

    last_signal: Optional[str] = api.state.get("last_signal")   # 恢复语义
    if api.is_resumed():
        api.log(f"恢复模式：上次信号={last_signal}，沿用 venue 对账持仓")
    # 已处理的最新**确认**极值点（timestamp 判重；pending 点不入信号）
    last_conf_ts: Optional[int] = api.state.get("last_conf_ts")
    last_bar_ts: Optional[int] = None
    bar_count = 0

    # ─── 阶段 1：主循环（runner 注入 @resilient，迭代异常续跑）───
    while api.wait_update(deadline=5.0):
        bars = api.klines(count=300)
        if len(bars) < 3:
            continue

        # ─── 阶段 2：判断新 K 线（收盘价模型）───
        cur_ts = int(bars[-1]["timestamp"])
        if last_bar_ts is not None and cur_ts == last_bar_ts:
            continue
        last_bar_ts = cur_ts
        bar_count += 1

        # ─── 阶段 3：生成信号（ZigZag 确认新极值点 = 状态机翻转）───
        confirmed = zz.confirmed_pivots
        if not confirmed:
            continue
        latest = confirmed[-1]              # 最新确认极值点（H 或 L）
        if last_conf_ts is not None and latest.timestamp <= last_conf_ts:
            continue                        # 无新确认点
        last_conf_ts = latest.timestamp

        if latest.direction == -1:          # 确认低点 L → 多头 leg 开启 → 做多
            signal = "long"
        else:                               # 确认高点 H → 空头 leg 开启 → 做空
            signal = "short"

        h1 = zigzag_high(zz)                # 函数形态接口示范（含 pending）
        l1 = zigzag_low(zz)
        ev = zz.last_cross
        api.log(
            f"[{bar_count}] 确认 {latest.label}@{latest.price:.5f} → {signal} | "
            f"H1={h1.label if h1 else '-'}@{h1.price:.5f} "
            f"L1={l1.label if l1 else '-'}@{l1.price:.5f} | "
            f"cross={ev.type + '@' + str(ev.index) if ev else '-'}"
        )

        if signal == last_signal:
            continue                        # 同向信号不重复开仓

        pos = api.position()

        # ─── 阶段 4：风控判断 + 仓位计算 ───
        try:
            atr_now = atr_v[-2]             # 最近已收盘 bar 的 ATR
        except IndexError:
            atr_now = None
        if atr_now is None or atr_now <= 0:
            api.log(f"[{bar_count}] ATR 无效（{atr_now}），跳过")
            continue
        stop_dist = Decimal(str(atr_now)) * Decimal(str(atr_mult)) + (info.pip_size or Decimal("0"))
        if stop_dist <= 0:
            api.log(f"[{bar_count}] stop_distance<=0，跳过")
            continue
        qty = api.calc_qty(risk_pct=risk_pct, stop_distance=stop_dist)
        if qty <= 0:
            api.log(f"[{bar_count}] calc_qty=0（风险 {risk_pct}% 对 stop {stop_dist} 过小），拒开")
            continue

        side_open = OrderSide.BUY if signal == "long" else OrderSide.SELL
        res = api.risk_check(sym, side_open, Offset.OPEN, qty)
        if not res.passed:
            api.log(
                f"[{bar_count}] 风控拒绝 OPEN {side_open.value} {qty}: "
                f"[{res.rule_name}] {res.reason} ({res.level})"
            )
            continue

        ref = Decimal(str(bars[-1]["close"]))
        rr = Decimal(str(rr_mult))
        if signal == "long":
            sl = ref - stop_dist
            tp = ref + stop_dist * rr
        else:
            sl = ref + stop_dist
            tp = ref - stop_dist * rr

        # ─── 阶段 5：发单（反手：先平反向仓，再开新仓）───
        if signal == "long" and pos.volume < 0:
            r = api.send_order(OrderSide.BUY, Offset.CLOSE, abs(pos.volume), kind=OrderKind.MARKET)
            api.log(f"  CLOSE SHORT: ok={r.ok} qty={r.filled_qty}@{r.filled_price} {r.reason}")
        elif signal == "short" and pos.volume > 0:
            r = api.send_order(OrderSide.SELL, Offset.CLOSE, pos.volume, kind=OrderKind.MARKET)
            api.log(f"  CLOSE LONG: ok={r.ok} qty={r.filled_qty}@{r.filled_price} {r.reason}")

        r = api.send_order(side_open, Offset.OPEN, qty, kind=OrderKind.MARKET, sl=sl, tp=tp)
        api.log(
            f"  OPEN {'LONG' if signal == 'long' else 'SHORT'}: ok={r.ok} qty={r.filled_qty}"
            f" sl={sl} tp={tp} reason={r.reason}"
        )

        last_signal = signal
        api.state["last_signal"] = signal
        api.state["last_conf_ts"] = last_conf_ts

    api.log(f"策略退出，共处理 {bar_count} 根新 bar")
