"""
# 导入后端
import kq_sdk

api = kq_sdk()
indic = api.INDIC()


# MACD策略

def macd_signal(symbol,period,draw=False):
    # MACD 注册
    fast=2,slow=5,m=2
    macd_1x,dif_1x,dea_1x = indic.macd(fast=fast,slow=slow,m=m)
    macd_4x,dif_4x,dea_4x = indic.macd(fast=fast*4,slow=slow*4,m=m*4)
    macd_16x,dif_16x,dea_16x = indic.macd(fast=fast*16,slow=slow*16,m=m*16)
    macd_64x,dif_64x,dea_64x = indic.macd(fast=fast*64,slow=slow*64,m=m*64)

    # MACD 信号逻辑
    if macd_16x > 0:
        if dea_1x < 0 and macd_1x > 0:
            position = api.get_position(symbol)
            # 持仓状态 0:表示没有持仓,-1:表示空单,1:表示多单
            if  position != 1:
                api.send_order(symbol=symbol, lot=2,type=1)
    
    # 处理OHLC
    bar = api.bar(symbol,period)
    open = bar.open()
    high = bar.high()
    close = bar.close()
    low = bar.low()
    
    for i in range(len(open)):
        '''
        这里处理K线逻辑,比如zigzag,K线形态
        '''

        pass
    

    # 多周期交叉验证
    if period == '1d':
        pass
    if period == 'H4':
        pass



    # 设置前端绘图
    if draw == True:
        # chart参数:0:主图,1:第一个副图,2:第二个副图,以些类推
        api.plot(chart=1,name='macd_v1',indicator=macd_1x,style='hist',thick=2,up_color=red,down_color=green)
        api.plot(chart=1,name='macd_v1',indicator=dif_1x,style='step_line',thick=1,up_color=red,down_color=green)
        api.plot(chart=1,name='macd_v1',indicator=dea_1x,style='line',thick=1,up_color=red,down_color=green)
        4x,16x,64x 代码省略...
"""
