"""M7 冷启动预热优化单测（超时降级 + 预热解耦）

覆盖规划 L227-231 验收：
  - 超时降级：预热用 warmup_timeout（patient）而非默认 8s 调 copy_rates，切断
    冷门品种首次拉取 >8s → worker 强杀 + 8s 冷却的连锁。
  - 预热解耦：degraded 品种不阻塞已就绪品种；后台节流重试；就绪后自动恢复接入。
  - 分批就绪：feed.start() 后循环内懒预热，无需同步等待全部品种历史加载完成。
"""
import time

from strategy.sdk.data_feed import Mt5DataFeed


def _rows(n=5, start_sec=1_700_000_000):
    return [
        {"time": start_sec + i * 60, "open": 1.1, "high": 1.11,
         "low": 1.09, "close": 1.10, "tick_volume": 10, "real_volume": 0}
        for i in range(n)
    ]


class _RecordingDriver:
    """记录 copy_rates_from_pos 的 timeout 参 + 可按品种配置 degraded / 延迟就绪。

    degraded:      永远返回 None 的品种集合（模拟冷门品种拉不到历史）。
    ready_after:   {symbol: N} 前 N 次调用返回 None、之后返回 rows（模拟延迟就绪）。
    """

    def __init__(self, degraded=None, ready_after=None, tick=None):
        self._degraded = set(degraded or ())
        self._ready_after = dict(ready_after or {})
        self._calls: dict[str, int] = {}
        self._tick = tick
        self.timeouts: list = []          # 每次 copy_rates 收到的 timeout
        self.select_calls: list[str] = []

    def symbol_select(self, symbol, enable=True):
        self.select_calls.append(symbol)
        return True

    def symbol_info_tick(self, symbol):
        return self._tick

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count, timeout=None):
        self.timeouts.append(timeout)
        if symbol in self._degraded:
            return None
        n = self._ready_after.get(symbol)
        if n is not None:
            self._calls[symbol] = self._calls.get(symbol, 0) + 1
            if self._calls[symbol] <= n:
                return None
        return _rows()


# ─── 超时降级 ───


def test_warm_symbol_passes_patient_timeout():
    """M7 超时降级：预热用 warmup_timeout（patient）而非默认 8s 调 copy_rates。"""
    drv = _RecordingDriver()
    feed = Mt5DataFeed(drv, symbols=["EURUSD"], periods=["1m"], warmup_timeout=30.0)
    assert feed._warm_symbol("EURUSD") is True
    # copy_rates 收到的 timeout == warmup_timeout（非 None / 非默认 8s）
    assert drv.timeouts and all(t == 30.0 for t in drv.timeouts)
    assert "EURUSD" in drv.select_calls        # 预热前 symbol_select


def test_warm_symbol_all_periods_must_load():
    """多周期：任一 period 无数据 → 整品种不就绪（避免半预热污染 is_changing）。"""

    class _HalfDriver(_RecordingDriver):
        def copy_rates_from_pos(self, symbol, timeframe, start_pos, count, timeout=None):
            self.timeouts.append(timeout)
            # 首个 period 有数据、其后无 → 整品种 degraded（不依赖 tf 常量值）
            return _rows() if len(self.timeouts) == 1 else None

    drv = _HalfDriver()
    feed = Mt5DataFeed(
        drv, symbols=["EURUSD"], periods=["1m", "1h"], warmup_timeout=30.0
    )
    assert feed._warm_symbol("EURUSD") is False


# ─── 预热解耦 ───


def test_degraded_symbol_does_not_block_ready_symbol():
    """M7 预热解耦：degraded 品种未就绪，但不影响其余品种就绪 + 加载 bars。"""
    drv = _RecordingDriver(degraded={"GBPUSD"})
    feed = Mt5DataFeed(drv, symbols=["EURUSD", "GBPUSD"], periods=["1m"])
    assert feed._try_warm("EURUSD") is True
    assert feed.is_ready("EURUSD") is True
    assert feed.latest_bars("EURUSD", "1m")            # 有 bars
    assert feed._try_warm("GBPUSD") is False
    assert feed.is_ready("GBPUSD") is False
    assert feed.latest_bars("GBPUSD", "1m") == []      # 无 bars


def test_degraded_symbol_auto_recovers_after_retry():
    """M7 验收：degraded 品种预热完成后自动恢复就绪（无需重启 feed）。"""
    drv = _RecordingDriver(ready_after={"GBPUSD": 1})   # 第 1 次 None、第 2 次 rows
    feed = Mt5DataFeed(
        drv, symbols=["GBPUSD"], periods=["1m"], warm_retry_interval=0.0
    )
    assert feed._try_warm("GBPUSD") is False            # 首次 degraded
    assert feed.is_ready("GBPUSD") is False
    assert feed._try_warm("GBPUSD") is True             # 重试成功 → 自动恢复
    assert feed.is_ready("GBPUSD") is True
    assert feed.latest_bars("GBPUSD", "1m")


def test_warm_retry_is_throttled():
    """M7：节流窗内不重复对 degraded 品种发起昂贵历史拉取（避免每轮 poll 猛击）。"""
    drv = _RecordingDriver(degraded={"EURUSD"})
    feed = Mt5DataFeed(
        drv, symbols=["EURUSD"], periods=["1m"], warm_retry_interval=60.0
    )
    assert feed._try_warm("EURUSD") is False            # 第 1 次：发起拉取
    n1 = len(drv.timeouts)
    assert feed._try_warm("EURUSD") is False            # 第 2 次：节流窗内 → 不拉取
    assert len(drv.timeouts) == n1                       # copy_rates 未再被调用


def test_ready_symbol_refreshes_heartbeat():
    """M7：预热就绪即刷新品种级 + 全局心跳（预热耗时不预先老化心跳）。"""
    drv = _RecordingDriver()
    feed = Mt5DataFeed(drv, symbols=["EURUSD"], periods=["1m"])
    old = time.monotonic() - 999.0
    feed._last_data_mono = old
    feed._last_symbol_mono["EURUSD"] = old
    assert feed._try_warm("EURUSD") is True
    assert feed.seconds_since_update("EURUSD") < 1.0    # 品种级心跳刷新
    assert feed.seconds_since_update() < 1.0            # 全局心跳刷新


# ─── 循环集成：懒预热 → 就绪 → 轮询 ───


def test_poll_loop_lazily_warms_then_polls():
    """M7 集成：feed.start() 后循环内懒预热，就绪品种接入实时轮询（分批就绪）。"""
    tick = {"bid": 1.1, "ask": 1.1001, "last": 0, "time": 1_700_000_000}
    drv = _RecordingDriver(tick=tick)
    feed = Mt5DataFeed(drv, symbols=["EURUSD"], periods=["1m"], poll_interval=0.02)
    assert feed.is_ready("EURUSD") is False             # 启动前未就绪
    feed.start()
    try:
        for _ in range(100):
            if feed.is_ready("EURUSD"):
                break
            time.sleep(0.02)
        assert feed.is_ready("EURUSD") is True
        assert feed.latest_bars("EURUSD", "1m")         # 预热加载了 bars
    finally:
        feed.stop()


def test_poll_loop_recovers_degraded_in_background():
    """M7 集成：degraded 品种后台重试就绪，不阻塞 feed；就绪后自动接入。"""
    tick = {"bid": 1.1, "ask": 1.1001, "last": 0, "time": 1_700_000_000}
    drv = _RecordingDriver(ready_after={"EURUSD": 2}, tick=tick)  # 前 2 次 None
    feed = Mt5DataFeed(
        drv, symbols=["EURUSD"], periods=["1m"],
        poll_interval=0.02, warm_retry_interval=0.0,
    )
    feed.start()
    try:
        for _ in range(150):
            if feed.is_ready("EURUSD"):
                break
            time.sleep(0.02)
        assert feed.is_ready("EURUSD") is True          # 后台重试后自动恢复
    finally:
        feed.stop()
