# ruff: noqa: E501, W291  # 底部 Pine 原版存档为逐字保留，豁免行长/尾随空格告警
"""Mister.Y cycle — 多倍数随机指标族（TradingView Pine v5 移植）

原版语义（Pine 见文件底部存档注释）：
    6 组 Stochastics（1x/16x/32x/64x/256x/1024x），每组
    k = ema(stoch(close,high,low,periodK), smoothK)、d = ema(k, periodD)；
    ta.stoch 手工组合：100*(close-rolling_min(low))/(rolling_max(high)-rolling_min(low))，
    分母为 0 时 polars 除法得 null，与 Pine 返回 na 语义一致。
    细节保真：1x 组 smoothK 含 ×2 因子；32x 组周期直接用 bet_32x（不乘 n）。
适配说明：
    - show_1x / show_max 布尔开关移除，各线常显（1024x 组原版默认隐藏，迁移后常显）
    - smooth_64x 参数与 show_16x 变量在原版为死代码（声明未用），丢弃
    - Pine 阶梯线（style_stepline）→ style.step=true，前端 primitive 自绘阶梯线还原
    - 带透明度颜色（#e01b7af6/#11cc0ef5/#c60eccf5、color.rgb(88,177,216,4)）
      契约只收 #RRGGBB，取实色
    - hline 的 linewidth 契约不支持，丢弃；线型保留（85/18 虚线，其余实线）
    - Pine 原版 32x 组标题与 1x 重复（%K/%D），字段统一命名 K_32X/D_32X
"""
from core.indicator_engine.graph import (
    ema,
    pyindicator,
    rolling_max,
    rolling_min,
)


def _stoch(close, high, low, period):
    """随机值 %K 原始序列：100*(close-LL)/(HH-LL)"""
    ll = rolling_min(low, period)
    hh = rolling_max(high, period)
    return (close - ll) / (hh - ll) * 100.0


@pyindicator(
    name="MR_Y_CYCLE", pane="sub", range="bounded_0_100",
    desc="Mister.Y 多倍数随机指标族（1x/16x/32x/64x/256x/1024x 的 K/D）",
    style=[
        {"color": "#d5e29b", "step": True},   # K_1X
        {"color": "#d28732", "step": True},   # D_1X
        {"color": "#0de8f3", "step": True},   # K_16X
        {"color": "#0c53c5", "step": True},   # D_16X
        {"color": "#d8a6d9", "step": True},   # K_32X
        {"color": "#f10f1e", "step": True},   # D_32X
        {"color": "#4ce11b", "step": True},   # K_64X
        {"color": "#ffa500", "step": True},   # D_64X
        {"color": "#aaa2c1", "step": True},   # K_256X
        {"color": "#8a2be2", "step": True},   # D_256X
        {"color": "#e01b7a", "step": True},   # K_1024X（Pine 原 #e01b7af6 取实色）
        {"color": "#00ffff", "step": True},   # D_1024X
    ],
    price_lines=[
        {"price": 85, "color": "#ffff00", "line_style": 2},   # Highest（虚线）
        {"price": 76, "color": "#787b86", "line_style": 0},   # Upper Band
        {"price": 61.8, "color": "#ff0000", "line_style": 0},  # Upper
        {"price": 58, "color": "#11cc0e", "line_style": 0},   # long_level
        {"price": 50, "color": "#58b1d8", "line_style": 0},   # mid
        {"price": 46, "color": "#c60ecc", "line_style": 0},   # short_level
        {"price": 38.2, "color": "#008000", "line_style": 0},  # Under
        {"price": 23.6, "color": "#787b86", "line_style": 0},  # Lower Band
        {"price": 18, "color": "#ffff00", "line_style": 2},   # low_level（虚线）
    ],
)
def mr_y_cycle(high, low, close, n1=1, n2=1, n3=1,
               bet_1x=3, bet_16x=13, bet_32x=24, bet_64x=48,
               bet_256x=192, bet_1024x=768):
    out = {}
    # 1x：smoothK 含 ×2 因子（原版 n2*bet_1x*2）
    k = ema(_stoch(close, high, low, n1 * bet_1x), n2 * bet_1x * 2)
    out["K_1X"] = k
    out["D_1X"] = ema(k, n3 * bet_1x)
    # 16x / 64x / 256x / 1024x：周期 = n × bet
    for bet, tag in ((bet_16x, "16X"), (bet_64x, "64X"),
                     (bet_256x, "256X"), (bet_1024x, "1024X")):
        k = ema(_stoch(close, high, low, n1 * bet), n2 * bet)
        out[f"K_{tag}"] = k
        out[f"D_{tag}"] = ema(k, n3 * bet)
    # 32x：周期直接用 bet_32x（原版不乘 n）
    k = ema(_stoch(close, high, low, bet_32x), bet_32x)
    out["K_32X"] = k
    out["D_32X"] = ema(k, bet_32x)
    return out


'''
// ─── TradingView Pine v5 原版存档 ───
//@version=5
indicator(title="Mister.Y cycle", shorttitle="cycle_3", format=format.price, precision=2, timeframe="", timeframe_gaps=true)
// 全局
//cur_n = input.int(defval = 3)
n1 = input.int(defval = 1,title ="N1", minval=1 ) // %length 
n2 = input.int(defval = 1,title=" N2", minval=1) // %K
n3 = input.int(defval = 1,title=" N3", minval=1) // %D

//smooth = input(defval = 2)
show_max = input(defval = false)
bet_1x  = input.int(defval = 3)
show_1x = input.bool(defval = true)
bet_16x = input.int(defval = 13)
//fix_16x = input(defval = 0)
bet_32x = input(defval = 24)
bet_64x = input.int(defval = 48)
smooth_64x = input(defval = 1)
bet_256x = input(defval = 192)
bet_1024x = input(defval = 768)


// Display Setup
h0 = hline(76, "Upper Band", color=#787B86)
high_1 = hline(85,'Highest',color = color.yellow,linestyle = hline.style_dashed)
high_level = hline(61.8,'Upper',color = color.red)
long_level = hline(58, "Middle Band", color=#11cc0ef5,linewidth = 1)
mid = hline(50, "Middle Band", color=color.rgb(88, 177, 216, 4),linewidth = 2)
short_level = hline(46, "Middle Band", color=#c60eccf5,linewidth = 1)
//mid_level = hline(38.2, "Middle Band", color=color.rgb(204, 14, 20, 4),linewidth = 4)
low_level = hline(38.2,'Under',color = color.green)
low_1 = hline(23.6, "Lower Band", color=#787B86)
low_2 = hline(18,'low_level',color=color.yellow,linestyle=hline.style_dashed)

//Calclation

//plot(upper_1 and boll ? upper_1:na, "Upper_1", color=#b1dd4c, offset = offset,style = plot.style_stepline)



// 4x
periodK_1x = n1*bet_1x
smoothK_1x = n2*bet_1x*2
periodD_1x = n3*bet_1x
k_1x= ta.ema(ta.stoch(close, high, low, periodK_1x), smoothK_1x)
//k_1x_2 = ta.ema(ta.stoch(close, high, low, 4), periodK_1x)*1.003
//k_1x = (k_1x_1+k_1x_2)/2 //可行性高
d_1x = ta.ema(k_1x, int(periodD_1x*1))
plot(k_1x and show_1x ? k_1x:na, title="%K", color=#d5e29b,style=plot.style_stepline) // #d697d2
plot(d_1x and show_1x ? d_1x:na, title="%D", color=#d28732,style=plot.style_stepline) // #f30dc1

// 16x

periodK_16x = n1*bet_16x
smoothK_16x = n2*bet_16x
periodD_16x = n3*bet_16x
//periodD_16x = n3*bet_16x+fix_16x


k_16x = ta.ema(ta.stoch(close, high, low, periodK_16x), smoothK_16x)
show_16x = ta.ema(ta.stoch(close, high, low, periodK_16x), smoothK_16x*2)
d_16x = ta.ema(k_16x, int(periodD_16x))

plot(k_16x, title="%16x_K", color=#0de8f3,style=plot.style_stepline) // #0c53c5
plot(d_16x, title="%16x_D", color=#0c53c5,style=plot.style_stepline) // #edbb09e0



//32x
periodK_32x = bet_32x
smoothK_32x = bet_32x
periodD_32x = bet_32x
k_32x = ta.ema(ta.stoch(close, high, low, periodK_32x), smoothK_32x)
d_32x = ta.ema(k_32x, int(periodD_32x))
//test_k_32x = ta.ema(k_32x, int(periodD_32x*0.382))
plot(k_32x, title="%K", color=#d8a6d9,style=plot.style_stepline) // #0c53c5
plot(d_32x, title="%D", color=#f10f1e,style=plot.style_stepline) // #edbb09e0


//  64x

periodK_64x = n1*bet_64x
smoothK_64x = n2*bet_64x
periodD_64x = n3*bet_64x
k_64x = ta.ema(ta.stoch(close, high, low, periodK_64x), smoothK_64x)
d_64x = ta.ema(k_64x, int(periodD_64x))//0.76
plot(k_64x, title="%64x_K", color=#4ce11b,style=plot.style_stepline) // #aaa2c1
plot(d_64x, title="%64x_D", color=#FFA500,style=plot.style_stepline) // #8A2BE2
//mid_64x = (k_64x + d_64x)/2
//plot(mid_64x, title="%mid_64x", color=color.rgb(0, 255, 200),style=plot.style_stepline)

// 256x
periodK_256x = n1*bet_256x
smoothK_256x = n2*bet_256x
periodD_256x = n3*bet_256x
k_256x = ta.ema(ta.stoch(close, high, low, periodK_256x), smoothK_256x)
d_256x = ta.ema(k_256x, int((periodD_256x)))

//hide_256x = (k_256x+d_256x)/2

plot(k_256x, title="%256x_K", color=#aaa2c1,style=plot.style_stepline) // color.white
plot(d_256x, title="%256x_D", color=#8A2BE2,style=plot.style_stepline) // #00FFFF
//hide_256x
//plot(hide_256x, title="%hid_64x", color=#1e52d6,style=plot.style_stepline) // #00FFFF

// 1024x
periodK_1024x = n1*bet_1024x
smoothK_1024x = n2*bet_1024x
periodD_1024x = n3*bet_1024x
k_1024x = ta.ema(ta.stoch(close, high, low, periodK_1024x), smoothK_1024x)
d_1024x = ta.ema(k_1024x, periodD_1024x)
plot(k_1024x and show_max ? k_1024x : na, title="%1024x_K", color=#e01b7af6,style=plot.style_stepline) // #e60d4e
plot(d_1024x and show_max ? d_1024x : na, title="%1024_D", color=#00FFFF,style=plot.style_stepline) // #33d5d8
'''
