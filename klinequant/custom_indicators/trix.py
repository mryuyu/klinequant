"""TRIX — 三重指数平滑动量振荡器（def 式自定义指标样例）

公式：TRIX = (EMA3 - EMA3前值) / EMA3前值 * 100，其中 EMA3 为对 close
连续三次 EMA 平滑。展示递推原语（ema）+ 向前引用（shift）+ 四则组合。
"""
from core.indicator_engine.graph import ema, pyindicator, shift


@pyindicator(name="TRIX", pane="sub", range="zero_symmetric",
             desc="三重指数平滑动量")
def trix(close, period=12):
    e3 = ema(ema(ema(close, period), period), period)
    prev = shift(e3, 1)
    return {"TRIX": (e3 - prev) / prev * 100.0}
