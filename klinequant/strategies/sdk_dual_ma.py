"""双均线标准示范策略（SDK 多策略配置化框架 · 需求 9）

外部 ``pip install kqsdk`` 后，写策略只需一行扁平导入（无需感知内部模块/protocol）::

    from kqsdk import strategy, KqApi, StrategyContext, OrderSide, Offset, OrderKind

    @strategy("dualma-eur")            # 策略名自定义，≠ 文件名
    def dual_ma(api: KqApi, ctx: StrategyContext) -> None:
        ...                             # 回测 / 实盘同一套代码

标准模板**五段式**：主循环 → 判断新 K 线 → 生成信号 → 风控判断 → 发单。
本文件是「回测/实盘同一套代码」的参考实现，逐段示范 11 条需求的 SDK 用法：

  需求 1  自定义指标：``api.INDIC().get("MR_Y_MAIN")`` / ``get("MACD_MINI")`` 作辅助确认
          （params.use_mry 开关，默认关）。
  需求 2  多标的/多周期：每个 (策略,品种) 一个 KqApi + 一线程，本函数只看自己的品种。
  需求 3  跨周期共享：1m 驱动，``api.INDIC("15m").ma(trend_ma)`` 直接读 15m 指标做趋势过滤。
  需求 4  持久化恢复：``api.is_resumed()`` 判定恢复态；上次信号方向存 ``api.state``，
          恢复后沿用 venue 对账的真实持仓，不盲目重开/加仓。
  需求 5  资金管理：``api.calc_qty(risk_pct=..., stop_distance=ATR×mult+点差)`` 按风险反推手数。
  需求 7  全局风控：发单前 ``api.risk_check(...).passed`` 预检（策略侧助手，不强制拦截）。
  需求 8/10 容错：主循环由 runner 注入的 ``@resilient`` 包装（单次迭代异常打日志续跑，
          连续超阈值才退出本策略线程，不拖垮其他策略）——策略体内无需显式包裹。
  需求 9  本文件即标准示范。
  需求 11 策略名自定义：``@kq_strategy("dualma-eur")`` 注册名 ≠ 文件名 sdk_dual_ma；
          同一模块可被 yaml 多个实例（dualma-eur / dualma-gbp…）以不同 name/params 复用。

驱动模型（反 look-ahead）：收盘价模型——仅当**新 bar 出现**（timestamp 变化）才评估；
信号取自最近两根**已收盘** bar（指标活视图 ``[-2]``/``[-3]``），成交落在当前揭示 bar，
绝不使用未收盘 bar 的指标值下单。

参数（全部来自 yaml → ``ctx.params``，不写死）：
  fast / slow         短/长均线周期（金叉死叉）
  trend_period        趋势过滤周期（跨周期，如 15m）
  trend_ma            趋势均线周期
  risk_pct            单笔风险 = 账户权益 × risk_pct%
  atr_period/atr_mult 止损距离 = ATR × atr_mult + 1 pip 点差缓冲
  rr_mult             盈亏比（tp = 入场价 ± stop_distance × rr_mult），默认 2.0
  use_mry             true 时附加 MR_Y_MAIN / MACD_MINI 确认（示范自定义指标）

运行：
  python scripts/run_strategies.py --config config/strategies.yaml --mode backtest
  python scripts/run_strategies.py --config config/strategies.yaml --mode live --account NAME
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

from kqsdk import FieldView, KqApi, Offset, OrderKind, OrderSide, StrategyContext
from kqsdk import strategy as kq_strategy


@kq_strategy("dualma-eur")
def dual_ma(api: KqApi, ctx: StrategyContext) -> None:
    """双均线策略主函数（双参形态 ``strategy(api, ctx)``，由 runner 调用）。"""
    # ─── 参数解析（需求 6：全部来自 yaml，不写死）───
    p = ctx.params or {}
    fast = int(p.get("fast", 7))
    slow = int(p.get("slow", 25))
    trend_period = str(p.get("trend_period", "15m"))
    trend_ma_n = int(p.get("trend_ma", 50))
    risk_pct = float(p.get("risk_pct", 1.0))
    atr_period = int(p.get("atr_period", 14))
    atr_mult = float(p.get("atr_mult", 1.5))
    rr_mult = float(p.get("rr_mult", 2.0))
    use_mry = bool(p.get("use_mry", False))
    warmup = max(slow, trend_ma_n, atr_period) + 10

    info = api.symbol_info()
    sym = info.symbol                     # 绑定品种（多品种 union 下不可用 symbols()[0]）
    api.log(
        f"=== {ctx.name} 双均线 MA({fast}/{slow}) 趋势={trend_period}:MA{trend_ma_n} "
        f"风险={risk_pct}% ATR({atr_period})×{atr_mult} rr={rr_mult} use_mry={use_mry} ==="
    )
    api.log(f"Spec[{sym}]: pip={info.pip_size} step={info.qty_step} min={info.min_qty}")

    # ─── 声明指标活视图（计算下沉 IndicatorEngine，四端同源同参；只声明一次）───
    indic = api.INDIC()                   # 主/驱动周期（1m）
    fast_ma = indic.ma(fast).field("MA")  # 内置 MA 字段名恒为 "MA"（非 "MA_7"）
    slow_ma = indic.ma(slow).field("MA")
    atr_v = indic.atr(atr_period).field("ATR")
    trend_ma = api.INDIC(trend_period).ma(trend_ma_n).field("MA")  # 需求 3：跨周期读 15m

    # 需求 1：自定义指标确认（可选；仅 use_mry 时声明，避免强制 764 根预热）
    mry_m4: Optional[FieldView] = None
    mry_e4: Optional[FieldView] = None
    mini_dif: Optional[FieldView] = None
    mini_dea: Optional[FieldView] = None
    if use_mry:
        mry = indic.get("MR_Y_MAIN")      # 主图均线系统（增量指标，字段名即输出键）
        mry_m4 = mry.field("M_4X")        # sma(12)
        mry_e4 = mry.field("E_4X")        # ema(12)
        mini = indic.get("MACD_MINI")
        mini_dif = mini.field("DIF_1X")
        mini_dea = mini.field("DEA_1X")

    # ─── 需求 4：恢复语义（重启/崩溃后从持久化状态续跑）───
    last_signal: Optional[str] = api.state.get("last_signal")  # "long"/"short"/None
    if api.is_resumed():
        pos0 = api.position()
        api.log(
            f"恢复模式：上次信号={last_signal}，venue 对账后持仓 vol={pos0.volume} "
            f"eff={pos0.effective}；沿用真实持仓，不盲目重开"
        )
    last_bar_ts: Optional[int] = None     # 已处理的最新 bar 时间戳（可重算，不持久化）
    bar_count = 0

    # ─── 阶段 1：主循环（需求 8/10：runner 注入 @resilient，迭代异常续跑）───
    while api.wait_update(deadline=5.0):
        bars = api.klines(count=max(warmup + 50, 300))
        if len(bars) < 3:
            continue

        # ─── 阶段 2：判断新 K 线（收盘价模型；同品种未出新 bar 则跳过）───
        #   多品种 lockstep 下 wait_update 会因其他品种/周期事件触发，
        #   用本品种 bars[-1] 时间戳变化过滤，天然只在自己的新 bar 上行动。
        cur_ts = int(bars[-1]["timestamp"])
        if last_bar_ts is not None and cur_ts == last_bar_ts:
            continue
        last_bar_ts = cur_ts
        bar_count += 1

        # 信号取最近两根**已收盘** bar（[-2]/[-3]）；[-1] 是仍在形成的 bar，禁用（反 look-ahead）
        f2, f3 = _val(fast_ma, -2), _val(fast_ma, -3)
        s2, s3 = _val(slow_ma, -2), _val(slow_ma, -3)
        if None in (f2, f3, s2, s3):
            continue

        # ─── 阶段 3：生成信号（金叉/死叉，基于已收盘 bar）───
        golden = f3 <= s3 and f2 > s2      # 短均线上穿长均线 → 做多
        death = f3 >= s3 and f2 < s2       # 短均线下穿长均线 → 做空
        if not golden and not death:
            continue
        signal = "long" if golden else "short"

        # 需求 3：跨周期趋势过滤——只在 15m 趋势方向一致时开仓（读已收盘 15m MA）
        tv = _val(trend_ma, -2)
        if tv is not None:
            price_ref = float(bars[-2]["close"])   # 最近已收盘 1m bar 收盘价
            if signal == "long" and price_ref < tv:
                api.log(
                    f"[{bar_count}] {signal} 被 {trend_period} 趋势过滤"
                    f"（price {price_ref:.5f} < MA{trend_ma_n} {tv:.5f}），跳过"
                )
                continue
            if signal == "short" and price_ref > tv:
                api.log(
                    f"[{bar_count}] {signal} 被 {trend_period} 趋势过滤"
                    f"（price {price_ref:.5f} > MA{trend_ma_n} {tv:.5f}），跳过"
                )
                continue

        # 需求 1：自定义指标确认（可选）——预热未完成或未确认则保守跳过
        if use_mry and not _mry_confirm(signal, mry_m4, mry_e4, mini_dif, mini_dea):
            api.log(f"[{bar_count}] {signal} 未通过 MR_Y_MAIN/MACD_MINI 确认，跳过")
            continue

        if signal == last_signal:
            continue                       # 同向信号不重复开仓

        pos = api.position()
        api.log(
            f"[{bar_count}] {'金叉' if golden else '死叉'} → {signal}: "
            f"MA{fast}[-2]={f2:.5f} MA{slow}[-2]={s2:.5f} | "
            f"pos vol={pos.volume} eff={pos.effective}"
        )

        # ─── 阶段 4：风控判断 + 仓位计算（需求 5/7）───
        atr_now = _val(atr_v, -2)          # 最近已收盘 bar 的 ATR
        if atr_now is None or atr_now <= 0:
            api.log(f"[{bar_count}] ATR 无效（{atr_now}），跳过")
            continue
        # 止损距离 = ATR × mult + 1 pip 点差缓冲
        stop_dist = Decimal(str(atr_now)) * Decimal(str(atr_mult)) + (info.pip_size or Decimal("0"))
        if stop_dist <= 0:
            api.log(f"[{bar_count}] stop_distance<=0（{stop_dist}），跳过")
            continue
        # 需求 5：按单笔风险百分比反推手数（不足 min_qty 返回 0 → 拒开）
        qty = api.calc_qty(risk_pct=risk_pct, stop_distance=stop_dist)
        if qty <= 0:
            api.log(f"[{bar_count}] calc_qty=0（风险 {risk_pct}% 对 stop {stop_dist} 过小），拒开")
            continue

        side_open = OrderSide.BUY if signal == "long" else OrderSide.SELL
        # 需求 7：发单前全局风控预检（先检后动；拒绝则保持原仓不变，等下次信号重试）
        res = api.risk_check(sym, side_open, Offset.OPEN, qty)
        if not res.passed:
            api.log(
                f"[{bar_count}] 风控拒绝 OPEN {side_open.value} {qty}: "
                f"[{res.rule_name}] {res.reason} ({res.level})"
            )
            continue

        # 灾难保险丝价位（需求 5：sl/tp 随开仓设一次；正常由策略本地逻辑先触发）
        ref = Decimal(str(bars[-1]["close"]))      # 当前揭示价 ≈ 成交价
        rr = Decimal(str(rr_mult))
        if signal == "long":
            sl = ref - stop_dist
            tp = ref + stop_dist * rr
        else:
            sl = ref + stop_dist
            tp = ref - stop_dist * rr

        # ─── 阶段 5：发单（反手：先平反向仓〔风险递减，始终放行〕，再开新仓）───
        if signal == "long" and pos.volume < 0:
            r = api.send_order(OrderSide.BUY, Offset.CLOSE, abs(pos.volume), kind=OrderKind.MARKET)
            api.log(
                f"  CLOSE SHORT: ok={r.ok} qty={r.filled_qty}"
                f"@{r.filled_price} reason={r.reason}"
            )
        elif signal == "short" and pos.volume > 0:
            r = api.send_order(OrderSide.SELL, Offset.CLOSE, pos.volume, kind=OrderKind.MARKET)
            api.log(
                f"  CLOSE LONG: ok={r.ok} qty={r.filled_qty}"
                f"@{r.filled_price} reason={r.reason}"
            )

        r = api.send_order(side_open, Offset.OPEN, qty, kind=OrderKind.MARKET, sl=sl, tp=tp)
        api.log(
            f"  OPEN {'LONG' if signal == 'long' else 'SHORT'}: ok={r.ok} qty={r.filled_qty}"
            f"@{r.filled_price} sl={sl} tp={tp} reason={r.reason}"
        )

        # 需求 4：持久化语义量（只存不可从 venue 重算的「上次信号方向」）
        last_signal = signal
        api.state["last_signal"] = signal
        api.state["last_signal_ts"] = cur_ts

    api.log(f"策略退出，共处理 {bar_count} 根新 bar")


def _val(fv: Optional[FieldView], idx: int):
    """安全取指标活视图历史值：视图为 None 或序列长度不足时返回 None（视作无信号）。

    跨周期/长周期指标（如 15m MA50）预热未完成时序列可能短于 |idx|，直接索引会
    IndexError；统一经此 helper 兜底，策略只判 None 即可（反 look-ahead 语义不变）。
    """
    if fv is None:
        return None
    try:
        return fv[idx]
    except IndexError:
        return None


def _mry_confirm(
    signal: str,
    m4: Optional[FieldView],
    e4: Optional[FieldView],
    dif: Optional[FieldView],
    dea: Optional[FieldView],
) -> bool:
    """需求 1 示范：MR_Y_MAIN(M_4X/E_4X) + MACD_MINI(DIF_1X/DEA_1X) 同向确认。

    读最近已收盘 bar（``[-2]``）；任一字段预热未完成（None）→ 保守返回 False（跳过）。
    long 需 M_4X>E_4X 且 DIF_1X>DEA_1X；short 反之。
    """
    vals = (_val(m4, -2), _val(e4, -2), _val(dif, -2), _val(dea, -2))
    if any(v is None for v in vals):
        return False
    m4v, e4v, difv, deav = vals
    if signal == "long":
        return m4v > e4v and difv > deav
    return m4v < e4v and difv < deav
