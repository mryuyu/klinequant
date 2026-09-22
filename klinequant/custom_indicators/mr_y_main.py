# ruff: noqa: E501, W291  # 底部 Pine 原版存档为逐字保留，豁免行长/尾随空格告警
"""Mister.Y MainChart — 主图均线系统 + 双通道布林（TradingView Pine v5 移植）

原版语义（Pine 见文件底部存档注释）：
    src = close，多组 SMA/EMA 配对均线（1x/4x/stop/16x/64x/256x），
    外加 length=280 的双通道布林带（±1.7σ 内轨 / ±2.7σ 外轨，basis 不画）。
适配说明：
    - Pine 阶梯线（style_stepline）→ style.step=true，前端 primitive 自绘阶梯线还原
    - 带透明度颜色（#e01b7af6）契约只收 #RRGGBB，取实色 #e01b7a
    - boll 布尔开关（控制带线显隐）移除，四条带线常显
    - offset 参数默认 0 未生效，丢弃
    - Pine 原版 M_16x/E_16x 标题重复（113 与 50 两组），
      113 组重命名为 M_STOP/E_STOP，50 组保留 M_16X/E_16X
"""
from core.indicator_engine.graph import ema, pyindicator, rolling_std, sma


@pyindicator(
    name="MR_Y_MAIN", pane="main", range="price",
    desc="Mister.Y 主图均线系统（多组 SMA/EMA + 双通道布林带）",
    style=[
        {"color": "#dbd789"},   # M_1X   sma(3)
        {"color": "#ea7f1b", "step": True},   # E_1X   ema(3) 阶梯线
        {"color": "#297fff"},   # M_4X   sma(12)
        {"color": "#70ffff", "step": True},   # E_4X   ema(12) 阶梯线
        {"color": "#b54e8c", "step": True},   # STOP_LINE ema(24) 阶梯线
        {"color": "#ed1515"},   # M_STOP sma(113)
        {"color": "#ea5c28", "step": True},   # E_STOP ema(113) 阶梯线
        {"color": "#ffa500"},   # M_16X  sma(50)
        {"color": "#19c613", "step": True},   # E_16X  ema(50) 阶梯线
        {"color": "#8a2be2"},   # M_64X  sma(192)
        {"color": "#d8bfd8", "step": True},   # E_64X  ema(192) 阶梯线
        {"color": "#00ffff"},   # M_256X sma(764)
        {"color": "#e01b7a", "step": True},   # E_256X ema(764) 阶梯线（Pine 原 #e01b7af6 取实色）
        {"color": "#b1dd4c", "step": True},   # UPPER_1 basis+1.7σ 阶梯线
        {"color": "#e4efee", "step": True},   # LOWER_1 basis-1.7σ 阶梯线
        {"color": "#d7a48a", "step": True},   # UPPER_2 basis+2.7σ 阶梯线
        {"color": "#67697e", "step": True},   # LOWER_2 basis-2.7σ 阶梯线
    ],
)
def mr_y_main(close, n=1, x0=3, main=12, stop_4x=24, stop_16x=113,
              bet_16x=48, bet_64x=192, bet_256x=764,
              bb_length=280, mult_1=1.7, mult_2=2.7):
    out = {}
    # 1x / 4x 均线组
    out["M_1X"] = sma(close, x0)
    out["E_1X"] = ema(close, x0)
    out["M_4X"] = sma(close, main)
    out["E_4X"] = ema(close, main)
    # 止损线（ema 24）与 113 组（Pine 原版标题误重复为 M_16x/E_16x）
    out["STOP_LINE"] = ema(close, stop_4x)
    out["M_STOP"] = sma(close, stop_16x)
    out["E_STOP"] = ema(close, stop_16x)
    # 16x / 64x / 256x 均线组（周期 = n × bet）
    for bet, tag in ((bet_16x, "16X"), (bet_64x, "64X"), (bet_256x, "256X")):
        p = int(n * bet)
        out[f"M_{tag}"] = sma(close, p)
        out[f"E_{tag}"] = ema(close, p)
    # 双通道布林带（basis = sma(bb_length)，basis 本身不输出）
    basis = sma(close, bb_length)
    std = rolling_std(close, bb_length)
    out["UPPER_1"] = basis + mult_1 * std
    out["LOWER_1"] = basis - mult_1 * std
    out["UPPER_2"] = basis + mult_2 * std
    out["LOWER_2"] = basis - mult_2 * std
    return out


'''
// ─── TradingView Pine v5 原版存档 ───
//@version=5
indicator(title='Mister.Y MainChart', shorttitle='MrY MainChart', overlay=true,format=format.price,precision=5) // timeframe='',
//import PineCoders/Time/4
//-----------------------------------------------
n = input.int(defval = 1,minval = 1,maxval = 999)
main = input.int(defval = 12,minval = 1,maxval = 999) 
x0 = input.int(defval = 3)
//HL_n = input(defval = 3)
//bet_4x = input(defval = 4)
bet_16x = input(defval =50)
bet_64x = input(defval = 192)
bet_256x = input(defval = 764)
stop_4x = input(defval = 24)
stop_16x = input(defval = 113)
//boll
boll = input.bool(defval = true)
length = input.int(280, minval=1)
src = close//(high+low+close)/3

//1x
plot(ta.sma(src,x0), color=#dbd789, title='m_1x')
plot(ta.ema(src,x0), color=#ea7f1b, title='e_1x', style=plot.style_stepline)


//4x
plot(ta.sma(src,main), color=#297fff, title='M_4x') //#2196f3
plot(ta.ema(src,main), color=#70ffff, title='E_4x', style=plot.style_stepline)
//----------- stop Line ------
// 23.6 = 24
plot(ta.ema(src,stop_4x), color=#b54e8c, title='stop_line', style=plot.style_stepline)
//plot(ta.ema(src,stop_4x+4), color=#a1e2e6, style=plot.style_stepline)
//38.2 = 144
plot(ta.sma(src,stop_16x), color=#ed1515, title='M_16x' )
plot(ta.ema(src,stop_16x), color=#ea5c28, title='E_16x',style=plot.style_stepline)


//16x 
plot(ta.sma(src,n*bet_16x), color=color.new(#FFA500, 0), title='M_16x' )
plot(ta.ema(src,n*bet_16x), color=color.new(#19c613, 0), title='E_16x',style=plot.style_stepline)



//64x 38.2
plot(ta.sma(src,n*bet_64x), color=color.new(#8A2BE2, 0), title='M_64x' )
plot(ta.ema(src,n*bet_64x), color=color.new(#D8BFD8, 0), title='E_64x', style=plot.style_stepline)

//256x
plot(ta.sma(src,n*bet_256x), color=color.new(#00FFFF, 0), title='M_256x')
plot(ta.ema(src,n*bet_256x), color=#e01b7af6, title='E_256x',style=plot.style_stepline)
//plot(ta.sma(src,int(n*bet_64x*4.236)), color=#d0ed16f6, title='E_256x',style=plot.style_steplinebr)

//boll_code
//maType = input.string("SMA", "Basis MA Type", options = ["SMA", "EMA", "SMMA (RMA)", "WMA", "VWMA"])
mult_1 = input.float(1.7,step=0.1, minval=0.1, maxval=8, title="in_Max")
mult_2 = input.float(2.7,step=0.1, minval=0.1, maxval=8, title="out_Max")
basis = ta.sma(src, length)
dev_1 = mult_1 * ta.stdev(src, length)
dev_2 = mult_2 * ta.stdev(src, length)
upper_1 = basis + dev_1
lower_1 = basis - dev_1
upper_2 = basis + dev_2
lower_2 = basis - dev_2
offset = input.int(0, "Offset", minval = -500, maxval = 500, display = display.data_window)
//plot(basis, "Basis", color=#2962FF, offset = offset)
plot(upper_1 and boll ? upper_1:na, "Upper_1", color=#b1dd4c, offset = offset,style = plot.style_stepline)
plot(lower_1 and boll ? lower_1:na, "Lower_1", color=#e4efee, offset = offset,style = plot.style_stepline)
//
plot(upper_2 and boll ? upper_2:na, "Upper_2", color=#d7a48a, offset = offset,style = plot.style_stepline)
plot(lower_2 and boll ? lower_2:na, "Lower_2", color=#67697e, offset = offset,style = plot.style_stepline)
'''
