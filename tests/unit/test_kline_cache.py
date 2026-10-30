"""进程级 K 线缓存（kline_cache）单元测试

覆盖：首拉建底 / 命中切片零拉取 / 尾部刷新覆盖未收盘 bar /
缺口补拉与去重 / 尽头判定 / LRU 淘汰。
"""
import asyncio

import pytest

from klinequant.gateway.market_sources import kline_cache as kc


def bar(ts, close=1.0):
    return {"timestamp": ts, "open": close, "high": close, "low": close,
            "close": close, "volume": 1.0}


H4, H1 = 14_400_000, 3_600_000   # v9.170 串周期测试：4H/1H 跨度(ms)


class FakeSource:
    """固定数据集源：按 end_time 过滤后返回最新 limit 根（升序），记录调用"""
    name = "fake"
    # 4h 原生直供（全局 NATIVE 集不含 4h，v9.170 串周期测试需原生跨度）
    supported_timeframes = {"1m", "1h", "4h", "1d"}

    def __init__(self, bars):
        self.data = sorted(bars, key=lambda b: b["timestamp"])
        self.calls = []   # [(limit, end_time)]

    async def fetch_klines(self, symbol, timeframe, limit=200, end_time=None):
        self.calls.append((limit, end_time))
        rows = [b for b in self.data if end_time is None or b["timestamp"] <= end_time]
        return rows[-limit:]

    def _track_prec(self, symbol, values):
        pass   # 派生聚合末尾推精度（生产源在 MarketSource 基类实现），假源空实现即可


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


def test_tail_refresh_updates_open_bar(monkeypatch):
    monkeypatch.setattr(kc, "_TAIL_REFRESH_TTL", 0.0)   # 隔离 TTL：本测专注尾刷覆盖未收盘 bar 机制
    src = FakeSource([bar(i) for i in range(10)])
    run(kc.cached_klines(src, "S", "1d", 5))
    # 未收盘 bar 变化（收盘抬高）
    src.data[-1] = bar(9, close=2.5)
    out = run(kc.cached_klines(src, "S", "1d", 5))
    assert out[-1]["close"] == 2.5 and len(out) == 5


def test_tail_refresh_ttl_collapses_burst():
    """尾刷 TTL：切换 burst 内已有存量的二次命中跳过重复尾刷（收敛公网源往返）"""
    src = FakeSource([bar(i) for i in range(10)])
    run(kc.cached_klines(src, "S", "1d", 5))            # 冷启动建底（无存量必拉）
    n = len(src.calls)
    out = run(kc.cached_klines(src, "S", "1d", 5))      # TTL 内二次命中：跳过尾刷
    assert len(out) == 5 and len(src.calls) == n        # 零新增源请求
    # TTL 过期后恢复尾刷（未收盘 bar 变化重新拉取）
    kc._cache[("fake", "S", "1d")].last_tail_at -= kc._TAIL_REFRESH_TTL + 1
    src.data[-1] = bar(9, close=3.0)
    out2 = run(kc.cached_klines(src, "S", "1d", 5))
    assert out2[-1]["close"] == 3.0 and len(src.calls) > n


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


def test_terminal_source_short_page_not_welded():
    """MT5 类源（history_end_on_short_page=False）：短页不永久焊死尽头。

    冷却内视同尽头不重复补拉；冷却过期后重试，终端侧加深历史后免重启即可续拉
    （XAUUSD 1m 卡 1038 根实证场景复现）。尾刷走默认 TTL（连续调用内跳过），聚焦补拉行为。"""
    src = FakeSource([bar(i) for i in range(100, 110)])  # 终端仅同步了浅层 10 根
    src.history_end_on_short_page = False
    out = run(kc.cached_klines(src, "S", "1d", 100))
    assert len(out) == 10                                # 短页 → 标尽头（带冷却）
    assert kc._cache[("fake", "S", "1d")].exhausted
    # 冷却内：再请求更深不再拉源（与焊死行为一致，防反复拉空页）
    n = len(src.calls)
    out2 = run(kc.cached_klines(src, "S", "1d", 100))
    assert len(out2) == 10 and len(src.calls) == n
    # 冷却过期 + 终端侧已加深历史：重试补拉拿到更深数据
    kc._cache[("fake", "S", "1d")].exhausted_at -= kc._EXHAUST_RETRY_COOLDOWN + 1
    src.data = [bar(i) for i in range(0, 110)]           # 模拟手动下载后历史变深
    out3 = run(kc.cached_klines(src, "S", "1d", 100))
    assert len(out3) == 100
    assert kc._cache[("fake", "S", "1d")].bars[0]["timestamp"] == 0


def test_rest_source_short_page_stays_welded_after_cooldown(monkeypatch):
    """REST 源（默认 history_end_on_short_page=True）：短页即尽头，冷却过期也不重试

    （防回归：币安/THS 源短页=回溯到上市日，重试只会反复拉空页）"""
    monkeypatch.setattr(kc, "_EXHAUST_RETRY_COOLDOWN", 0.0)   # 冷却归零也不得重试
    src = FakeSource([bar(i) for i in range(0, 10)])
    run(kc.cached_klines(src, "S", "1d", 100))
    n = len(src.calls)
    out2 = run(kc.cached_klines(src, "S", "1d", 100, end_time=3))
    assert len(out2) == 4 and len(src.calls) == n


class ShortPageTerminalSource(FakeSource):
    """模拟 MT5 终端源：补拉窗口被周末/休市空洞吃掉后单次至多返请求量 1/4（短页），
    但更早历史仍充足——短页不得判尽头，须继续翻页凑满（XAUUSD 1m 跨周末 748/1000 实证）"""
    history_end_on_short_page = False

    async def fetch_klines(self, symbol, timeframe, limit=200, end_time=None):
        self.calls.append((limit, end_time))
        rows = [b for b in self.data if end_time is None or b["timestamp"] <= end_time]
        return rows[-max(1, limit // 4):]


def test_terminal_source_short_page_keeps_paging_until_filled():
    """终端源短页（窗口内合法空洞）不中断补拉：多轮翻页凑满请求深度"""
    src = ShortPageTerminalSource([bar(i) for i in range(0, 10_000)])
    out = run(kc.cached_klines(src, "S", "1d", 1000))
    assert len(out) == 1000                        # 旧逻辑首轮短页即断，只能拿到 ~500 根
    assert out[0]["timestamp"] == 9000 and out[-1]["timestamp"] == 9999
    e = kc._cache[("fake", "S", "1d")]
    assert not e.exhausted                         # 短页≠尽头，未误标
    assert len(src.calls) <= kc._FILL_MAX_ROUNDS + 1   # 轮数护栏内


def test_terminal_source_deep_paging_crosses_weekend_gap():
    """终端源翻页越过周末空洞拿到更早历史（缓存最旧根前是 >1 个窗口的休市段）"""
    # 本周 500 根（ts 10_000起）+ 上周 2000 根（ts 0起），中间 8000 跨度空洞（仿周末+长假）
    data = [bar(i) for i in range(0, 2000)] + [bar(i) for i in range(10_000, 10_500)]
    src = FakeSource(data)
    src.history_end_on_short_page = False
    e = kc._entry(("fake", "S", "1d"))
    e.bars = [dict(b) for b in data[-500:]]        # 缓存仅本周段
    e.exhausted = True                             # 旧逻辑曾在此焊死
    e.exhausted_at = 0.0                           # 冷却已过 → 允许重试
    out = run(kc.cached_klines(src, "S", "1d", 1200))
    assert len(out) == 1200                        # 跨过空洞补齐到上周段
    assert out[0]["timestamp"] == 1300             # 已触及上周段（1300~1999 + 本周 500 根）


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


# ─── 内部中空洞自愈（源掉线遗留、尾刷/前补都修不了的缺口） ───


def test_interior_gap_repaired_from_source():
    """缓存中段有洞但源有完整数据：命中最新时补拉回填，恢复连续"""
    full = [bar(i * 60_000) for i in range(100)]          # 连续 1m
    src = FakeSource(full)
    e = kc._entry(("fake", "S", "1m"))
    e.bars = [dict(b) for b in (full[:40] + full[60:])]   # 中段挖空 40~59
    e.exhausted = True                                    # 前补已到源头，仅剩中段缺口
    out = run(kc.cached_klines(src, "S", "1m", 100))
    ts = [b["timestamp"] for b in out]
    assert len(out) == 100 and ts == sorted(ts) and len(set(ts)) == 100
    for i in range(40, 60):                               # 缺口已回填
        assert i * 60_000 in ts


def test_interior_gap_repaired_with_end_time_paging():
    """翻页（end_time 在过去）时，服务窗口内的中段缺口同样补拉回填、恢复连续"""
    full = [bar(i * 60_000) for i in range(120)]
    src = FakeSource(full)
    e = kc._entry(("fake", "S3", "1m"))
    e.bars = [dict(b) for b in (full[:40] + full[60:])]   # 中段挖空 40~59
    e.exhausted = True
    out = run(kc.cached_klines(src, "S3", "1m", 60, end_time=100 * 60_000))
    assert len(out) == 60
    diffs = {out[i + 1]["timestamp"] - out[i]["timestamp"] for i in range(len(out) - 1)}
    assert diffs == {60_000}                              # 完全连续，无缺口


def test_legit_gap_empty_source_not_refetched_within_cooldown(monkeypatch):
    """源确无数据的合法空洞：补拉返空后缺口保留，冷却内二次请求不再拉源补洞"""
    monkeypatch.setattr(kc, "_TAIL_REFRESH_TTL", 0.0)   # 隔离 TTL：冷却内仅尾刷 1 次、不重复补洞
    data = [bar(i * 60_000) for i in range(40)] + [bar(i * 60_000) for i in range(60, 100)]
    src = FakeSource(data)                                # 源本身就带洞（周末式）
    e = kc._entry(("fake", "S2", "1m"))
    e.bars = [dict(b) for b in data]
    e.exhausted = True
    out = run(kc.cached_klines(src, "S2", "1m", 100))
    assert len(out) == 80                                 # 洞仍在
    n = len(src.calls)
    out2 = run(kc.cached_klines(src, "S2", "1m", 100))    # 冷却内再取
    assert len(out2) == 80
    assert len(src.calls) - n == 1                        # 仅尾刷 1 次，无补洞拉取


def test_interval_ms_and_derived_gate():
    """周期跨度解析：实际走派生聚合的档返回 None（不做补洞）；日/周线 >= _DAY_MS 亦不触发补洞；
    格式可解析但源原生直供的档（如币安 4h）按原生跨度参与补洞/尽头判定"""
    assert kc._interval_ms("1m") == 60_000
    assert kc._interval_ms("5m") == 300_000
    assert kc._interval_ms("1h") == 3_600_000
    assert kc._interval_ms("1d") == 86_400_000            # 解析成功但 >= _DAY_MS，不补洞
    assert kc._interval_ms("1w") == 604_800_000
    assert kc._interval_ms("2d") is None                  # 自定义倍率派生档
    assert kc._interval_ms("10m") is None                 # 分钟级自定义倍率（1m 重采样）
    assert kc._interval_ms("3h") is None                  # 小时级自定义倍率
    assert kc._interval_ms("1M") is None                  # 月线派生档
    assert kc._interval_ms("1Q") is None
    assert kc._interval_ms("1Y") is None

    # 按源判定：同是 4h，原生直供源按跨度解析，缺供源（如 ths）走派生返 None
    class _Src:
        def __init__(self, tfs):
            self.supported_timeframes = tfs
    binance_like = _Src({"1m", "1h", "2h", "4h", "1d"})
    ths_like = _Src({"1m", "5m", "1h", "1d"})
    assert kc._interval_ms("4h", binance_like) == 14_400_000
    assert kc._interval_ms("4h", ths_like) is None


def test_derived_populates_and_shares_base_cache():
    """派生周期经缓存层时，基础 K 落入 kline_cache 并被后续请求零源复用（回归锁）

    修复前 fetch_derived_klines 内部直连 source.fetch_klines 拉基础 K，不建
    ("fake","S","1m") 缓存条目，致 K 线路由与每个指标预热各自冷拉一遍基础 K
    （公网源经代理串行 → 派生周期指标 20s+ 突破前端 30s 超时不显示）。
    修复后基础 K 经注入的 cached_klines 共享，20000 根 1m 拉一次即被多请求复用。
    """
    src = FakeSource([bar(i * 60_000) for i in range(300)])   # 300 根连续 1m（可聚 30 个 10m 桶）
    out = run(kc.cached_klines(src, "S", "10m", 20))           # 派生档请求（K 线路由口径）
    assert len(out) == 20 and all(b["timestamp"] % 600_000 == 0 for b in out)
    # 基础 1m 已经注入的 cached_klines 落盘共享（修复前此条目不存在）
    base = kc._cache.get(("fake", "S", "1m"))
    assert base is not None and len(base.bars) == 300
    # 后续对基础 1m 的请求命中共享存量：尾刷 TTL 内零新增源请求（证明基础 K 被复用）
    n = len(src.calls)
    out2 = run(kc.cached_klines(src, "S", "1m", 300))
    assert len(out2) == 300 and len(src.calls) == n


def test_subinterval_contaminants_dropped_on_merge():
    """v9.170 串周期污染防火墙：4H 序列混入 1H bar 合入时剔除，真 bar 全保留。
    基线带时区偏移（ts%4h≡1h，实测 MT5 形态）证明口径是间距而非网格对齐"""
    base = 1_760_000_000_000 - (1_760_000_000_000 % H4) + H1
    clean = [bar(base + i * H4) for i in range(40)]
    polluted = list(clean)
    for i in range(20, 30):                      # 污染段：10 根真 bar 后各插 3 根 1H
        for k in (1, 2, 3):
            polluted.append(bar(base + i * H4 + k * H1))
    src = FakeSource(polluted)
    out = run(kc.cached_klines(src, "S", "4h", 100))
    assert [b["timestamp"] for b in out] == [b["timestamp"] for b in clean]


def test_polluted_entry_self_heals_on_next_request():
    """v9.170：存量污染缓存下次请求自愈（合入后整表统一收口剔除子间隔 bar）"""
    base = 1_760_100_000_000 - (1_760_100_000_000 % H4)
    clean = [bar(base + i * H4) for i in range(40)]
    src = FakeSource(clean)
    e = kc._entry(("fake", "S3", "4h"))
    polluted = list(clean)
    for i in range(20, 30):
        for k in (1, 2, 3):
            polluted.append(bar(base + i * H4 + k * H1))
    e.bars = sorted(polluted, key=lambda b: b["timestamp"])
    out = run(kc.cached_klines(src, "S3", "4h", 100))
    assert [b["timestamp"] for b in out] == [b["timestamp"] for b in clean]


def test_subinterval_filter_keeps_legit_gaps_and_majority_guard():
    """v9.170 防火墙边界：合法缺口（周末 52h，间距只更大）不误剔；
    序列自身子间隔（整页污染/合成时间戳）多数护栏豁免不过滤"""
    base = 1_760_000_000_000
    ts_list = [base + i * H4 for i in range(20)] + [base + 33 * H4 + i * H4 for i in range(20)]
    src = FakeSource([bar(t) for t in ts_list])
    out = run(kc.cached_klines(src, "S2", "4h", 100))
    assert [b["timestamp"] for b in out] == ts_list
    dense = [bar(base + i * 3_600_000) for i in range(40)]   # 整页 1H：剔除将过半 → 原样返回
    assert kc._filter_subinterval(dense, H4) == dense
    assert kc._filter_subinterval(dense, None) == dense     # 派生档 interval_ms=None 不过滤


# ─── 时区口径代次（MT5 会话中途校正服务器 UTC 偏移，同一时间戳落到两根不同真实时刻）───

M1, SHIFT = 60_000, 3 * 3600 * 1000   # 1m 跨度 / 服务器 UTC+3 未校正时的 epoch 超前量（ms）


class TzSource:
    """带 tz_gen 代次的源：bars 按代次给不同数据集，模拟偏移校正前后同一时间戳换内容。

    flip_after=N 时第 N 次 fetch 返回后递一代次（模拟请求处理中途探测生效）。
    """
    name = "tzfake"
    supported_timeframes = {"1m", "1d"}

    def __init__(self, bars_by_gen):
        self.bars_by_gen = bars_by_gen
        self.tz_gen = 0
        self.calls: list[tuple] = []
        self.flip_after = None

    async def fetch_klines(self, symbol, timeframe, limit=200, end_time=None):
        self.calls.append((limit, end_time))
        if self.flip_after is not None and len(self.calls) == self.flip_after:
            self.tz_gen += 1
        rows = self.bars_by_gen.get(self.tz_gen, [])
        rows = [b for b in rows if end_time is None or b["timestamp"] <= end_time]
        return rows[-limit:]


def _tz_data():
    """gen0：未校正 epoch（墙钟口径）的 120 根 close=100；gen1：校正后同键区间的 60 根
    close=200。两者键完全重叠（实证故障形态），代次护栏缺失时旧 60 根会与新 60 根交替共存"""
    base = 1_760_000_000_000 - (1_760_000_000_000 % M1)
    old = [bar(base + SHIFT + i * M1, 100.0) for i in range(120)]
    new = [bar(base + SHIFT + i * M1, 200.0) for i in range(60)]
    return old, new


def test_tz_gen_change_drops_stale_epoch_bars():
    """代次变化 → 旧口径整条作废重建，不得残留任何旧 epoch bar（按间隔判洞的自愈对此全盲）"""
    old, new = _tz_data()
    src = TzSource({0: old, 1: new})
    out = run(kc.cached_klines(src, "S", "1m", 60))
    assert all(b["close"] == 100.0 for b in out)      # 先按 gen0 建底
    assert kc._entry(("tzfake", "S", "1m")).tz_gen == 0

    src.tz_gen = 1
    out2 = run(kc.cached_klines(src, "S", "1m", 120))
    assert [b["close"] for b in out2] == [200.0] * 60   # 旧 120 根全部作废，只剩新口径
    assert kc._entry(("tzfake", "S", "1m")).tz_gen == 1


def test_tz_gen_shift_mid_request_restarts_once():
    """请求中途代次变化：已合入 bar 混两种 epoch → 整条作废并重启一次取干净结果"""
    old, new = _tz_data()
    src = TzSource({0: old, 1: new})
    src.flip_after = 1                                 # 首拉返回后才校正生效
    out = run(kc.cached_klines(src, "S", "1m", 60))
    assert len(src.calls) == 2                         # 污染请求被丢弃 + 重建
    assert [b["close"] for b in out] == [200.0] * 60   # 不返混合结果


def test_tz_gen_consecutive_shift_raises():
    """二次仍变化（极端：连续夏令时抖动/配置错）不无限重启，上抛由各调用方兜底"""
    _, new = _tz_data()
    src = TzSource({})

    async def _flip_every_call(symbol, timeframe, limit=200, end_time=None):
        src.tz_gen += 1                       # 每次拉取都变代次 → 重启一次仍不收敛
        return new

    src.fetch_klines = _flip_every_call
    with pytest.raises(kc._TzGenShiftError):
        run(kc.cached_klines(src, "S", "1m", 60))


def test_source_without_tz_gen_attr_unaffected():
    """无偏移校正概念的源（币安/ths/测试桩）无 tz_gen 属性 → 恒代次 0，不触发任何作废"""
    src = FakeSource([bar(i * M1) for i in range(100)])
    assert not hasattr(src, "tz_gen")
    out = run(kc.cached_klines(src, "S", "1m", 50))
    assert len(out) == 50
    assert kc._entry(("fake", "S", "1m")).tz_gen == 0
    out2 = run(kc.cached_klines(src, "S", "1m", 50))
    assert out2 == out
