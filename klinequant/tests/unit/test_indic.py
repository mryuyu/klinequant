"""Phase 1 声明式指标（api.INDIC() → IndicatorEngine）单测。

覆盖《SDK 阶段实施规划 v1.3》Phase 1 验收项：
  - 声明预热：``indic.macd(...)`` 从 feed 已加载 bars 整批预热，活视图立即有值；
  - 增量推进：feed 逐 bar 前进经 ``advance()`` 快照法增量，有效序列随之增长；
  - 视图标量与序列：``view.dif[-1]`` / ``float()`` / 比较 / 解包取标量，``view.series`` 取整段序列；
  - ``is_changing(view)``：KqApi 经 ``_kq_bar_key`` 映射到底层 bar 版本 key；
  - 同参对拍 gateway：SDK 增量结果 == 引擎整批 warmup（indicator_service 同法），四端同源同参；
  - 边界：冷启动无 bars、无 engine、多参数隔离、advance 幂等、自定义指标经 get()。

回测数据用注入的合成 bars（BacktestDataFeed 回放），不依赖 MT5 / 网络。
"""
import math
from decimal import Decimal

import pytest

from core.indicator_engine.engine import IndicatorEngine
from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from core.trade_engine.spec_loader import load_spec_from_mt5_dict
from protocol.types import Offset, OrderKind, OrderSide
from strategy.sdk.api import KqApi
from strategy.sdk.backtest_executor import BacktestExecutor
from strategy.sdk.backtest_feed import BacktestDataFeed
from strategy.sdk.backtest_runner import BacktestRunner
from strategy.sdk.indic import FieldView, IndicApi, IndicatorView, bars_to_df

BASE_TS = 1_600_000_000_000
_INFO = {
    "digits": 5, "point": 0.00001, "trade_contract_size": 100000,
    "volume_step": 0.01, "volume_min": 0.01, "volume_max": 200,
    "trade_mode": 4, "spread": 0,
    "currency_base": "EUR", "currency_profit": "USD", "margin_currency": "USD",
}
_MACD_PARAMS = {"fast_period": 2, "slow_period": 5, "signal_period": 2}


# ─── 脚手架 ───


def _spec(symbol: str = "TESTUSD"):
    return load_spec_from_mt5_dict(_INFO, symbol)


def _mk_bars(n: int, symbol: str = "TESTUSD", period: str = "1m"):
    """确定性合成 bar 序列（正弦 + 线性趋势，避免全平导致指标退化）。"""
    bars = []
    prev_close = 1.10
    for i in range(n):
        close = 1.10 + 0.01 * math.sin(i / 3.0) + i * 0.0002
        open_ = prev_close
        high = max(open_, close) + 0.0005
        low = min(open_, close) - 0.0005
        bars.append({
            "symbol": symbol, "period": period, "timestamp": BASE_TS + i * 60000,
            "open": open_, "high": high, "low": low, "close": close, "volume": 1000.0,
        })
        prev_close = close
    return bars


def _feed(bars, period: str = "1m") -> BacktestDataFeed:
    return BacktestDataFeed({"TESTUSD": bars}, period)


def _engine() -> IndicatorEngine:
    eng = IndicatorEngine()
    eng.start()
    return eng


def _replay(feed: BacktestDataFeed, n: int) -> None:
    """推进 feed n 根（不接 IndicApi，仅让 latest_bars 可见 n 根历史）。"""
    for _ in range(n):
        if not feed.wait_update():
            break


def _wire_api(bars, with_engine: bool = True):
    """组装 feed + executor + KqApi（回测同构；滑点/手续费置零）。"""
    spec = _spec()
    feed = BacktestDataFeed({"TESTUSD": bars}, "1m")
    ex = BacktestExecutor(
        feed=feed, specs={"TESTUSD": spec}, initial_capital=Decimal("10000"),
        slippage_model="fixed", slippage_params={"ticks": Decimal("0")},
        fee_model="fixed", fee_params={"fee_per_trade": Decimal("0")},
    )
    eng = _engine() if with_engine else None
    api = KqApi(
        symbol="TESTUSD", period="1m", tag="1m", specs={"TESTUSD": spec},
        ledger=ExposureLedger(), resolver=UnifiedResolver(), executor=ex, feed=feed,
        engine=eng, exchange="mt5",
    )
    return feed, api, eng


# ─── 声明预热 ───


def test_declare_warms_up_from_loaded_bars():
    """feed 已有历史 bars 时声明 → 整批预热，活视图立即有值（声明预热）。"""
    feed = _feed(_mk_bars(60))
    _replay(feed, 40)                       # index=39，40 根可见
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")
    view = indic.macd(fast=2, slow=5, m=2)
    assert isinstance(view, IndicatorView)
    assert view.fields == ["DIF", "DEA", "HIST"]
    assert view.dif.last is not None        # 预热完成即有值
    assert len(view.series) == 40 - 6 + 1   # min_periods=slow+signal-1=6
    eng.stop()


def test_declare_registers_indicator_in_engine():
    """声明把指标注册进共享引擎（四端同源同参的物理基础）。"""
    feed = _feed(_mk_bars(30))
    _replay(feed, 30)
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")
    view = indic.macd(2, 5, 2)
    assert eng.has_indicators("TESTUSD", "mt5", "1m")
    assert view.ind_key == eng.ind_key("MACD", _MACD_PARAMS)
    eng.stop()


# ─── 视图标量与序列 ───


def test_field_view_scalar_and_series():
    """标量访问（[-1]/float()/比较）+ 序列访问（values/series/迭代解包）。"""
    feed = _feed(_mk_bars(60))
    _replay(feed, 60)
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")
    view = indic.macd(2, 5, 2)

    dif, dea, hist = view                       # 按字段序解包
    assert [f.field for f in (dif, dea, hist)] == ["DIF", "DEA", "HIST"]
    assert all(isinstance(f, FieldView) for f in (dif, dea, hist))

    # 标量：[-1] == last == float()
    assert dif[-1] == dif.last
    assert float(dif) == pytest.approx(dif.last)
    # 序列：values == series 投影
    assert dif.values == [it["values"]["DIF"] for it in view.series]
    assert len(dif) == len(view.series)

    # 字段访问三写法等价（属性 / 下标 / 大小写不敏感 field()）
    assert view.dif.field == view["DIF"].field == view.field("dif").field == "DIF"

    # 标量比较作用于最新值
    assert (hist > 0) == (hist.last > 0)
    assert (hist < 0) == (hist.last < 0)
    assert (dif > dea) == (dif.last > dea.last)
    eng.stop()


def test_view_none_before_warmup():
    """冷启动无 bars（index=-1）声明：最新值 None，有序比较一律 False，float() 抛错。"""
    feed = _feed(_mk_bars(60))               # 未 replay，index=-1
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")
    view = indic.macd(2, 5, 2)
    dif = view.dif
    assert dif.last is None
    assert bool(dif) is False
    assert (dif > 0) is False
    assert (dif < 0) is False
    assert (dif >= 0) is False
    assert (dif <= 0) is False
    with pytest.raises(ValueError):
        float(dif)
    eng.stop()


def test_view_field_access_errors():
    """视图契约：非字符串下标抛 TypeError，未知字段属性抛 AttributeError。"""
    feed = _feed(_mk_bars(30))
    _replay(feed, 30)
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")
    view = indic.macd(2, 5, 2)
    with pytest.raises(TypeError):
        _ = view[0]
    with pytest.raises(AttributeError):
        _ = view.not_a_field
    eng.stop()


# ─── 增量推进 ───


def test_incremental_advance_grows_series():
    """declare 于 index=-1，逐 bar advance() → 有效序列单调增长，末值非空（增量推进）。"""
    feed = _feed(_mk_bars(60))
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")
    view = indic.macd(2, 5, 2)               # 声明时无 bars，预热留待 advance 补
    assert view.dif.last is None

    lengths = []
    while feed.wait_update():
        indic.advance()
        lengths.append(len(view.series))
    # 有效值出现后序列随回放增长，末根 = 60 - min_periods(6) + 1
    assert lengths[-1] == 60 - 6 + 1
    assert lengths[-1] > lengths[5]           # 单调增长
    assert view.dif.last is not None
    eng.stop()


def test_advance_idempotent_same_bar():
    """同一根 bar 反复 advance()：值变门控命中 → 幂等，序列不重复推进。"""
    feed = _feed(_mk_bars(30))
    _replay(feed, 20)
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")
    view = indic.macd(2, 5, 2)               # warmup 20 根
    len0, last0 = len(view.series), view.dif.last
    indic.advance()                          # index 未变 → 末根同 ts/H/L/C → no-op
    indic.advance()
    assert len(view.series) == len0
    assert view.dif.last == last0
    eng.stop()


def test_advance_warms_pending_when_bars_arrive_late():
    """冷启动竞态：declare 早于 bars 就绪 → advance() 在拿到 bars 后补预热。"""
    feed = _feed(_mk_bars(40))
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")
    view = indic.macd(2, 5, 2)
    assert view.dif.last is None             # 声明时无 bars
    assert feed.wait_update() is True        # 推进到 index=0（1 根）
    indic.advance()                          # 补预热（1 根，仍不足 min_periods）
    # 继续推进直到超过 min_periods，补预热后增量出值
    for _ in range(10):
        assert feed.wait_update() is True
        indic.advance()
    assert view.dif.last is not None
    eng.stop()


# ─── 同参对拍 gateway（四端同源同参） ───


def test_sdk_incremental_matches_gateway_batch():
    """SDK 逐 bar 增量 == 引擎整批 warmup（indicator_service.ensure_warmed 同法）。

    证明「同一 registry + 同 (name, params)」下，SDK 轨与 gateway 轨输出一致。
    """
    bars = _mk_bars(80)

    # SDK 轨：declare + 逐 bar advance（增量快照法）
    feed = _feed(bars)
    eng_sdk = _engine()
    indic = IndicApi(eng_sdk, feed, "TESTUSD", "mt5", "1m")
    view = indic.macd(2, 5, 2)
    while feed.wait_update():
        indic.advance()
    sdk = view.series

    # gateway 轨：ensure_indicator + 整批 warmup(only_key)
    eng_gw = _engine()
    eng_gw.ensure_indicator("MACD", _MACD_PARAMS, "TESTUSD", "mt5", "1m")
    eng_gw.warmup(
        "TESTUSD", "mt5", "1m", bars_to_df(bars),
        only_key=eng_gw.ind_key("MACD", _MACD_PARAMS),
    )
    gw = eng_gw.get_series("MACD", _MACD_PARAMS, "TESTUSD", "mt5", "1m")

    assert len(sdk) == len(gw) > 0
    assert [it["timestamp"] for it in sdk] == [it["timestamp"] for it in gw]
    for a, b in zip(sdk, gw):
        for f in ("DIF", "DEA", "HIST"):
            assert a["values"][f] == pytest.approx(b["values"][f], rel=1e-9, abs=1e-12)
    eng_sdk.stop()
    eng_gw.stop()


# ─── is_changing 映射 ───


def test_is_changing_maps_view_to_bar_key():
    """KqApi.is_changing(view/field) 经 _kq_bar_key 映射到底层 bar 版本 key。"""
    feed, api, eng = _wire_api(_mk_bars(30))
    view = api.INDIC().macd(2, 5, 2)
    assert view._kq_bar_key == "TESTUSD/1m"
    assert api.wait_update() is True          # bump "TESTUSD/1m" + 桥接 advance
    assert api.is_changing(view) is True
    assert api.is_changing(view.dif) is True
    eng.stop()


def test_wait_update_bridges_advance():
    """KqApi.wait_update() 每轮数据到达后桥接推进指标（Live/Backtest 同构入口）。"""
    feed, api, eng = _wire_api(_mk_bars(40))
    view = api.INDIC().macd(2, 5, 2)
    assert view.dif.last is None              # index=-1
    n = 0
    while api.wait_update():
        n += 1
    assert n == 40
    assert view.dif.last is not None          # 桥接 advance 后出值
    eng.stop()


# ─── KqApi.INDIC() 契约 ───


def test_kqapi_indic_requires_engine():
    """runner 未注入 engine 时调用 INDIC() 抛 RuntimeError（清晰失败）。"""
    feed, api, _ = _wire_api(_mk_bars(10), with_engine=False)
    with pytest.raises(RuntimeError):
        api.INDIC()


def test_kqapi_indic_cached_singleton():
    """INDIC() 惰性构造并缓存：多次调用返回同一 IndicApi 实例。"""
    feed, api, eng = _wire_api(_mk_bars(10))
    assert api.INDIC() is api.INDIC()
    eng.stop()


# ─── 多参数隔离 + 别名 + 自定义注册表 ───


def test_multiple_params_isolated():
    """同指标多参数组共存：ind_key 不同、活视图各读各的、互不串值。"""
    feed = _feed(_mk_bars(120))
    _replay(feed, 120)
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")
    fast = indic.macd(2, 5, 2)
    slow = indic.macd(12, 26, 9)
    assert fast.ind_key != slow.ind_key
    assert fast.dif.last != slow.dif.last
    assert len(eng.indicators_for("TESTUSD", "mt5", "1m")) == 2
    eng.stop()


def test_builtin_aliases_and_custom_registry():
    """别名方法（ma/ema/rsi/boll）+ 通用 get() 走注册表（含自定义 DEMA）均出活视图。"""
    feed = _feed(_mk_bars(120))
    _replay(feed, 120)
    eng = _engine()
    indic = IndicApi(eng, feed, "TESTUSD", "mt5", "1m")

    views = [
        indic.ma(period=5),
        indic.ema(period=10),
        indic.rsi(14),
        indic.boll(20, 2.0),
        indic.get("EMA", period=10),          # 通用 get 与别名同参 → 同 ind_key
        indic.get("DEMA"),                    # def 式自定义指标（注册表全量可达）
    ]
    for v in views:
        assert isinstance(v, IndicatorView)
        assert len(v.series) > 0
        first = v.field(v.fields[0])
        assert first.last is not None

    # get("EMA", period=10) 与 ema(period=10) 计算契约同 key（幂等复用同一实例）
    assert views[1].ind_key == views[4].ind_key
    # DEMA 自定义字段名
    assert "DEMA" in views[5].fields
    eng.stop()


# ─── 端到端：demo 式 4 组参数 MACD 策略经 BacktestRunner 跑通（验收#1 回测轨） ───


def test_backtest_runner_end_to_end_4param_macd():
    """demo 式 4 组参数 MACD 策略经 BacktestRunner 端到端跑通。

    同一 strategy 声明 4 个 MACD 参数组，在 wait_update 循环里读活视图标量做
    信号比较并下单，验证「runner 建引擎 → KqApi.INDIC() → wait_update 桥接
    advance → 4 组视图出值 → 标量比较驱动下单」整链贯通（回测/实盘同构）。
    """
    bars = _mk_bars(120)
    seen = {"declared": 0, "all4_with_values": 0, "comparisons": 0, "orders": 0}

    def strategy(api):
        indic = api.INDIC()
        # demo 意图：同指标多参数组（快/中/标准/慢）并行声明，各读各的活视图
        m_quick = indic.macd(fast=2, slow=5, m=2)
        m_mid = indic.macd(fast=4, slow=10, m=4)
        m_std = indic.macd(fast=12, slow=26, m=9)
        m_slow = indic.macd(fast=8, slow=20, m=8)
        views = [m_quick, m_mid, m_std, m_slow]
        seen["declared"] = len(views)
        position = None
        while api.wait_update():
            hists = [v.hist.last for v in views]        # 标量读（活视图最新值）
            if any(h is None for h in hists):
                continue
            seen["all4_with_values"] += 1
            seen["comparisons"] += 1
            bullish = m_std.hist > 0 and m_quick.hist > 0   # 标量比较（信号）
            bearish = m_std.hist < 0 and m_quick.hist < 0
            if bullish and position is None:
                api.send_order(OrderSide.BUY, Offset.OPEN, Decimal("0.01"),
                               kind=OrderKind.MARKET)
                position = "long"
                seen["orders"] += 1
            elif bearish and position == "long":
                api.send_order(OrderSide.SELL, Offset.CLOSE, Decimal("0.01"),
                               kind=OrderKind.MARKET)
                position = None
                seen["orders"] += 1

    runner = BacktestRunner(
        symbols=["TESTUSD"], period="1m", strategy_fn=strategy,
        initial_capital=Decimal("10000"),
        slippage_model="fixed", slippage_params={"ticks": Decimal("0")},
        fee_model="fixed", fee_params={"fee_per_trade": Decimal("0")},
        bars_by_symbol={"TESTUSD": bars}, specs={"TESTUSD": _spec()},
    )
    report = runner.run()
    res = report.results["TESTUSD"]

    assert res.n_bars == 120
    assert seen["declared"] == 4                 # 4 组参数并行声明
    assert seen["all4_with_values"] > 0          # 引擎被 wait_update 桥接推进，视图出值
    assert seen["comparisons"] > 0               # 标量比较路径被执行
    assert seen["orders"] >= 1                   # 信号驱动下单贯通
    assert len(res.equity_curve) > 0
