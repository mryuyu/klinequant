"""MisterY_MACD_trend — MACD 多倍数组合（TradingView Pine v5 移植）

原版语义（Pine 见文件底部存档注释）：
    基础参数 fast=12 / slow=20 / signal=9，按倍数展开 1X/4X/16X/64X：
    - 1X：柱 MCD = 2*(DIF-DEA)（四色：零轴上增 #0f9d8f / 上缩 #B2DFDB /
      下增 #FFCDD2 / 下缩 #EF5350）+ DIF + DEA
    - 4X/16X/64X：仅 DIF + DEA（周期 = 基础周期 × 倍数）
适配说明：
    - Pine 阶梯线（style_stepline）lwc 无对应线型，降级实线
    - 带透明度颜色（#FFA500@8%、#e01b7af6）契约只收 #RRGGBB，取实色
"""
from core.indicator_engine.graph import ema, pyindicator


@pyindicator(
    name="MACD_MULTI", pane="sub", range="zero_symmetric",
    desc="MACD 多倍数（1X 柱 + 4X/16X/64X 的 DIF/DEA）",
    style=[
        {"plot": "histogram",   # 1X 柱：四槽色 = 零轴上增/上缩/下增/下缩
         "hist_colors": ["#0f9d8f", "#B2DFDB", "#FFCDD2", "#EF5350"]},
        {"color": "#20e3d6"},   # DIF_1X（Pine 原为阶梯线）
        {"color": "#2962ff"},   # DEA_1X
        {"color": "#19c613"},   # DIF_4X
        {"color": "#ffa500"},   # DEA_4X
        {"color": "#d8bfd8"},   # DIF_16X
        {"color": "#8a2be2"},   # DEA_16X
        {"color": "#e01b7a"},   # DIF_64X
        {"color": "#00ffff"},   # DEA_64X
    ],
    price_lines=[{"price": 0}],   # 零轴参考线（缺省灰色虚线）
)
def macd_multi(close, s=12, p=20, m=9):
    out = {}
    for mult, tag in ((1, "1X"), (4, "4X"), (16, "16X"), (64, "64X")):
        dif = ema(close, s * mult) - ema(close, p * mult)
        dea = ema(dif, m * mult)
        if mult == 1:
            out[f"MCD_{tag}"] = (dif - dea) * 2.0   # 仅 1X 出柱
        out[f"DIF_{tag}"] = dif
        out[f"DEA_{tag}"] = dea
    return out


'''
// ─── TradingView Pine v5 原版存档 ───
//@version=5
indicator(title='MisterY_MACD_trend',format = format.price, precision=8,shorttitle='MACD_trend', timeframe='')
s = input(defval = 12)
p = input(defval = 20)
m = input.int(defval = 9)
bet_4x  = input(defval = 4)
bet_16x = input(defval = 16)
bet_64x = input(defval = 64)

// 1x
fast_ema_1x = ta.ema(close, s)
slow_ema_1x = ta.ema(close, p)
dif_1x = fast_ema_1x - slow_ema_1x
dea_1x = ta.ema(dif_1x, m)
mcd_1x = 2*(dif_1x - dea_1x)
plot(mcd_1x, title='MACD Histogram', style=plot.style_columns, color=mcd_1x >= 0 ? mcd_1x[1] < mcd_1x ? #0f9d8f : #B2DFDB : mcd_1x[1] < mcd_1x ? #FFCDD2 : #EF5350)
plot(dif_1x, title='diff', color=#20e3d6,style=plot.style_stepline)
plot(dea_1x, title='dea', color=#2962FF)

// 4x / 16x / 64x：周期 = 基础 × 倍数，仅画 DIF/DEA
fast_ma_4x = ta.ema(close, s*bet_4x)
dif_4x = fast_ma_4x - ta.ema(close, p*bet_4x)
plot(dif_4x, color=color.new(#19c613, 0),style=plot.style_stepline)
plot(ta.ema(dif_4x, m*bet_4x), color=color.new(#FFA500, 8))
fast_ma_16x = ta.ema(close, s*bet_16x)
dif_16x = fast_ma_16x - ta.ema(close, p*bet_16x)
plot(dif_16x, color=#D8BFD8,style=plot.style_stepline)
plot(ta.ema(dif_16x, m*bet_16x), color=#8A2BE2)
fast_ma_64x = ta.ema(close, s*bet_64x)
dif_64x = fast_ma_64x - ta.ema(close, p*bet_64x)
plot(dif_64x, color=#e01b7af6,style=plot.style_stepline)
plot(ta.ema(dif_64x, m*bet_64x), color=#00FFFF)
'''
