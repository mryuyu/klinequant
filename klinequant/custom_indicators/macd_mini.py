# ruff: noqa: E501, W291  # 底部 Pine 原版存档为逐字保留，豁免行长/尾随空格告警
"""MACD_mini — MACD 多倍数组合（TradingView Pine v5 移植）

原版语义（Pine 见文件底部存档注释）：
    基础参数 fast=2 / slow=5 / signal=3，按倍数展开 1X/4X/16X/64X：
    - 1X：柱 MCD = 2*(DIF-DEA)（四色：零轴上增 #0f9d8f / 上缩 #B2DFDB /
      下增 #FFCDD2 / 下缩 #EF5350）+ DIF（阶梯线）+ DEA
    - 4X/16X/64X：仅 DIF（阶梯线）+ DEA（周期 = 基础周期 × 倍数）
适配说明：
    - Pine 阶梯线（plot.style_stepline）→ style.step=true，仅 DIF 线声明，
      由前端 Series Primitive 自绘还原（bar 内水平、下一 bar 跳变）
    - mid_64x = (dif_64x+dea_64x)*0.382 在原版声明后未 plot，为死代码，丢弃
    - 与 macd_multi 同族，仅默认周期（2/5/3）与 DIF/DEA 配色不同
"""
from core.indicator_engine.graph import ema, pyindicator


@pyindicator(
    name="MACD_MINI", pane="sub", range="zero_symmetric",
    desc="MACD_mini 多倍数（1X 柱 + 4X/16X/64X 的 DIF/DEA，DIF 阶梯线）",
    style=[
        {"plot": "histogram",   # 1X 柱：四槽色 = 零轴上增/上缩/下增/下缩
         "hist_colors": ["#0f9d8f", "#B2DFDB", "#FFCDD2", "#EF5350"]},
        {"color": "#ecebb1", "step": True},   # DIF_1X（Pine 阶梯线）
        {"color": "#9f6e0d"},                 # DEA_1X
        {"color": "#20e3d6", "step": True},   # DIF_4X
        {"color": "#2962ff"},                 # DEA_4X
        {"color": "#19c613", "step": True},   # DIF_16X
        {"color": "#ffa500"},                 # DEA_16X
        {"color": "#d8bfd8", "step": True},   # DIF_64X
        {"color": "#8a2be2"},                 # DEA_64X
    ],
    price_lines=[{"price": 0}],   # 零轴参考线（缺省灰色虚线）
)
def macd_mini(close, s=2, p=5, m=3):
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
indicator(title='MACD_mini',format = format.price, precision=6,shorttitle='MACD_mini', timeframe='')
// 全局变量
s = input(defval = 2,title = 'fast')
p = input(defval = 5,title = 'slow')
m = input(defval = 3)

bet_4x  = input(defval = 4)
bet_16x = input(defval = 16)
bet_64x = input(defval = 64)

// 1x
fast_length_1x = s
slow_length_1x = p
src = close
signal_length_1x = m
fast_ema_1x = ta.ema(src, fast_length_1x)
slow_ema_1x = ta.ema(src, slow_length_1x)
dif_1x = fast_ema_1x - slow_ema_1x
dea_1x = ta.ema(dif_1x, signal_length_1x)
mcd_1x = (dif_1x - dea_1x)*2
plot(mcd_1x, title='MACD Histogram', style=plot.style_columns, color=mcd_1x >= 0 ? mcd_1x[1] < mcd_1x ? #0f9d8f : #B2DFDB : mcd_1x[1] < mcd_1x ? #FFCDD2 : #EF5350)
plot(dif_1x, title='diff', color=#ecebb1,style=plot.style_stepline)
plot(dea_1x, title='dea', color=#9f6e0d)

// 4x
fast_length_4x = s*bet_4x
slow_length_4x = p*bet_4x
signal_length_4x = m*bet_4x
fast_ma_4x = ta.ema(src, fast_length_4x)
slow_ma_4x = ta.ema(src, slow_length_4x)
dif_4x = fast_ma_4x - slow_ma_4x
dea_4x = ta.ema(dif_4x, signal_length_4x)
mcd_4x = dif_4x - dea_4x
plot(dif_4x, title='diff', color=#20e3d6,style=plot.style_stepline)
plot(dea_4x, title='dea', color=#2962FF)

// 16x
fast_length_16x = s*bet_16x
slow_length_16x = p*bet_16x
signal_length_16x = m*bet_16x
fast_ma_16x = ta.ema(src, fast_length_16x)
slow_ma_16x = ta.ema(src, slow_length_16x)
dif_16x = fast_ma_16x - slow_ma_16x
dea_16x = ta.ema(dif_16x, signal_length_16x)
mcd_16x = dif_16x - dea_16x
plot(dif_16x, title='diff', color=#19c613,style=plot.style_stepline)
plot(dea_16x, title='dea', color=#FFA500)

// 64x
fast_length_64x = s*bet_64x
slow_length_64x = p*bet_64x
signal_length_64x = m*bet_64x
fast_ma_64x = ta.ema(src, fast_length_64x)
slow_ma_64x = ta.ema(src, slow_length_64x)
dif_64x = fast_ma_64x - slow_ma_64x
dea_64x = ta.ema(dif_64x, signal_length_64x)
mcd_64x = dif_64x - dea_64x
mid_64x = (dif_64x+dea_64x)*0.382   // 原版声明后未 plot，死代码
plot(dif_64x, title='96', color=#D8BFD8,style=plot.style_stepline)
plot(dea_64x, title='192', color=#8A2BE2)
'''
