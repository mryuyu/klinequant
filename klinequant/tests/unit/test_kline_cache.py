"""进程级 K 线缓存（kline_cache）单元测试

覆盖：首拉建底 / 命中切片零拉取 / 尾部刷新覆盖未收盘 bar /
缺口补拉与去重 / 尽头判定 / LRU 淘汰。
"""
import asyncio

import pytest

from gateway.market_sources import kline_cache as kc


def bar(ts, close=1.0):
    return {"timestamp": ts, "open": close, "high": close, "low": close,
            "close": close, "volume": 1.0}


class FakeSource:
    """固定数据集源：按 end_time 过滤后返回最新 limit 根（升序），记录调用"""
    name = "fake"

    def __init__(self, bars):
        self.data = sorted(bars, key=lambda b: b["timestamp"])
        self.calls = []   # [(limit, end_time)]

    async def fetch_klines(self, symbol, timeframe, limit=200, end_time=None):
        self.calls.append((limit, end_time))
        rows = [b for b in self.data if end_time is None or b["timestamp"] <= end_time]
        return rows[-limit:]


@pytest.fixture(autouse=True)
def _clean_cache():
    kc.clear()
    yield
    kc.clear()


def run(coro):
    return asyncio.run(coro)


def test_first_fetch_then_hit_sliced_no_full_refetch():
    src = FakeSource([bar(i) for i in range(100, 200)])
    # 首拉最新 30 根（尾刷路径建底）
    out = run(kc.cached_klines(src, "S", "1d", 30))
    assert len(out) == 30 and out[-1]["timestamp"] == 199
    # 翻旧页（end_time 在缓存最旧根之前）→ 触发一次补拉后切片
    out2 = run(kc.cached_klines(src, "S", "1d", 30, end_time=169))
    assert len(out2) == 30 and out2[-1]["timestamp"] == 169
    n_calls_after_fill = len(src.calls)
    # 同页再取：纯命中，零拉取
    out3 = run(kc.cached_klines(src, "S", "1d", 30, end_time=169))
    assert out3 == out2 and len(src.calls) == n_calls_after_fill


def test_tail_refresh_updates_open_bar():
    src = FakeSource([bar(i) for i in range(10)])
    run(kc.cached_klines(src, "S", "1d", 5))
    # 未收盘 bar 变化（收盘抬高）
    src.data[-1] = bar(9, close=2.5)
    out = run(kc.cached_klines(src, "S", "1d", 5))
    assert out[-1]["close"] == 2.5 and len(out) == 5


def test_fill_merges_without_duplicates():
    src = FakeSource([bar(i) for i in range(0, 50)])
    run(kc.cached_klines(src, "S", "1d", 20))          # 缓存 30~49
    out = run(kc.cached_klines(src, "S", "1d", 40))    # 需要补到 10~49
    assert len(out) == 40
    ts = [b["timestamp"] for b in out]
    assert ts == sorted(ts) and len(set(ts)) == 40


def test_exhausted_stops_further_fetches():
    src = FakeSource([bar(i) for i in range(0, 10)])   # 全量仅 10 根
    out = run(kc.cached_klines(src, "S", "1d", 100))
    assert len(out) == 10
    n = len(src.calls)
    # 已判定尽头：再请求更深不再拉源
    out2 = run(kc.cached_klines(src, "S", "1d", 100, end_time=3))
    assert len(out2) == 4 and len(src.calls) == n


def test_lru_eviction(monkeypatch):
    monkeypatch.setattr(kc, "_MAX_ENTRIES", 2)
    src = FakeSource([bar(i) for i in range(10)])
    run(kc.cached_klines(src, "A", "1d", 5))
    run(kc.cached_klines(src, "B", "1d", 5))
    run(kc.cached_klines(src, "C", "1d", 5))           # 挤出 A
    assert ("fake", "A", "1d") not in kc._cache
    assert ("fake", "B", "1d") in kc._cache


def test_fetch_failure_keeps_partial_cache():
    src = FakeSource([bar(i) for i in range(50)])
    run(kc.cached_klines(src, "S", "1d", 20))

    async def boom(symbol, timeframe, limit=200, end_time=None):
        raise RuntimeError("source down")
    src.fetch_klines = boom
    # 补拉失败：退回已有存量（≤44 仅 15 根），异常不向上抛；不标尽头（源恢复后可继续）
    out = run(kc.cached_klines(src, "S", "1d", 30, end_time=44))
    assert len(out) == 15
    assert not kc._cache[("fake", "S", "1d")].exhausted
