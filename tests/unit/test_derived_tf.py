"""网关派生周期层（derived）单元测试

覆盖：
    - parse_tf：固定档/自定义倍率（含分钟·小时级）解析与非法格式拒绝
    - base_tf_for / resolve_tf：聚合基础周期与按源路由判定
    - bucket_label：月/季/年/周倍率/日倍率桶标签（北京时区、跨边界）及日内 epoch 对齐
    - aggregate_daily：OHLC 合并/量求和/末桶进行中输出（日 K 与 1m 基础同口径）
    - base_need：基础 K 拉取量换算与封顶
    - fetch_derived_klines：假源拉基础 K 聚合、limit 截取、end_time 过滤（日级/日内）
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from klinequant.gateway.market_sources.derived import (
    FETCH_DAILY_CAP,
    FETCH_INTRADAY_CAP,
    aggregate_daily,
    base_need,
    base_tf_for,
    bucket_label,
    daily_need,
    fetch_derived_klines,
    is_derived,
    parse_tf,
    resolve_tf,
)

_BJ = timezone(timedelta(hours=8))


def _bj_ms(y: int, m: int, d: int) -> int:
    return int(datetime(y, m, d, tzinfo=_BJ).timestamp() * 1000)


def _day(ts_ms: int, o: float, h: float, lo: float, c: float, v: float) -> dict:
    return {"timestamp": ts_ms, "open": o, "high": h, "low": lo, "close": c, "volume": v}


# ─── parse_tf / is_derived ───

def test_parse_tf_fixed():
    assert parse_tf("1M") == (1, "M")
    assert parse_tf("1Q") == (1, "Q")
    assert parse_tf("1Y") == (1, "Y")


def test_parse_tf_custom():
    assert parse_tf("2d") == (2, "d")
    assert parse_tf("3w") == (3, "w")
    assert parse_tf("2M") == (2, "M")
    assert parse_tf("99d") == (99, "d")


def test_parse_tf_intraday_custom():
    """分钟/小时级自定义倍率（日内重采样扩展）"""
    assert parse_tf("2m") == (2, "m")
    assert parse_tf("10m") == (10, "m")
    assert parse_tf("59m") == (59, "m")
    assert parse_tf("3h") == (3, "h")
    assert parse_tf("24h") == (24, "h")


@pytest.mark.parametrize(
    "tf", ["100M", "0d", "1m", "1d", "1w", "15m", "abc", "", "2q", "1.5d", "100m", "0h"],
)
def test_parse_tf_reject(tf):
    assert parse_tf(tf) is None
    assert not is_derived(tf)


def test_is_derived_true():
    for tf in ("1M", "1Q", "1Y", "2d", "3w", "4M", "2m", "10m", "3h"):
        assert is_derived(tf)


# ─── base_tf_for / resolve_tf ───

def test_base_tf_for():
    assert base_tf_for("10m") == "1m"
    assert base_tf_for("3h") == "1m"
    assert base_tf_for("2d") == "1d"
    assert base_tf_for("1M") == "1d"
    with pytest.raises(ValueError):
        base_tf_for("1d")   # 原生档非派生


_THS_LIKE = {"1m", "5m", "15m", "30m", "1h", "1d", "1w"}
_BINANCE_LIKE = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "3d", "1w"}


def test_resolve_tf_native_passthrough():
    """源原生直供的档透传（含格式可解析的 2h/4h，不双重口径）"""
    assert resolve_tf("4h", _BINANCE_LIKE) is None
    assert resolve_tf("2h", _BINANCE_LIKE) is None
    assert resolve_tf("1m", _BINANCE_LIKE) is None
    assert resolve_tf("1w", _THS_LIKE) is None


def test_resolve_tf_derived_routing():
    """源不供给且格式可派生 → 返回聚合基础周期；不可派生且不支持 → None（路由层拒绝）"""
    assert resolve_tf("4h", _THS_LIKE) == "1m"     # ths 无 4h → 1m 重采样兜底
    assert resolve_tf("8h", _THS_LIKE) == "1m"     # 8≤99 倍率格式可派生
    assert resolve_tf("10m", _BINANCE_LIKE) == "1m"
    assert resolve_tf("2d", _THS_LIKE) == "1d"
    assert resolve_tf("1M", _THS_LIKE) == "1d"
    assert resolve_tf("7x", _THS_LIKE) is None     # 非法格式且源不支持
    assert resolve_tf("1000m", _THS_LIKE) is None  # 倍率超两位数字，格式非法


# ─── bucket_label ───

def test_bucket_month():
    p = parse_tf("1M")
    assert bucket_label(_bj_ms(2026, 8, 31), p) == _bj_ms(2026, 8, 1)
    assert bucket_label(_bj_ms(2026, 1, 1), p) == _bj_ms(2026, 1, 1)
    assert bucket_label(_bj_ms(2025, 12, 15), p) == _bj_ms(2025, 12, 1)


def test_bucket_quarter():
    p = parse_tf("1Q")
    assert bucket_label(_bj_ms(2026, 8, 15), p) == _bj_ms(2026, 7, 1)
    assert bucket_label(_bj_ms(2026, 2, 10), p) == _bj_ms(2026, 1, 1)
    assert bucket_label(_bj_ms(2026, 12, 31), p) == _bj_ms(2026, 10, 1)


def test_bucket_year():
    p = parse_tf("1Y")
    assert bucket_label(_bj_ms(2026, 5, 1), p) == _bj_ms(2026, 1, 1)
    assert bucket_label(_bj_ms(2013, 1, 4), p) == _bj_ms(2013, 1, 1)


def test_bucket_nd_align_epoch():
    p = parse_tf("2d")
    # 1970-01-01 为对齐起点：第 2 天归首桶，第 3 天开新桶
    assert bucket_label(_bj_ms(1970, 1, 2), p) == _bj_ms(1970, 1, 1)
    assert bucket_label(_bj_ms(1970, 1, 3), p) == _bj_ms(1970, 1, 3)


def test_bucket_nw_align_monday():
    p = parse_tf("2w")
    # 2026-08-31 为周一；相邻周二同桶，两周后的周一开新桶
    base = bucket_label(_bj_ms(2026, 8, 31), p)
    assert bucket_label(_bj_ms(2026, 9, 1), p) == base          # 周二同桶
    assert bucket_label(_bj_ms(2026, 9, 14), p) == base + 14 * 86400_000
    # 桶标签必为周一（1969-12-29 对齐）
    lab = datetime.fromtimestamp(base / 1000, tz=_BJ)
    assert lab.weekday() == 0


def test_bucket_intraday_epoch_align():
    """分钟/小时级按 epoch 整除对齐（UTC 口径，不随交易时段切分）"""
    t0 = _bj_ms(2026, 10, 27)   # 当日零点（北京），恰为整小时 epoch 边界
    p10 = parse_tf("10m")
    assert bucket_label(t0, p10) == t0
    assert bucket_label(t0 + 9 * 60_000, p10) == t0            # 第 10 分钟才翻滚
    assert bucket_label(t0 + 10 * 60_000, p10) == t0 + 600_000
    p3 = parse_tf("3h")
    # 北京零点 = UTC 16:00，落在 UTC 3h 桶 [15:00,18:00) 内：桶起点为 t0-3600s
    assert bucket_label(t0, p3) == t0 - 3_600_000
    assert bucket_label(t0 + 119 * 60_000, p3) == t0 - 3_600_000   # 至 UTC 17:59 同桶
    assert bucket_label(t0 + 121 * 60_000, p3) == t0 + 2 * 3_600_000   # UTC 18:01 落入下一桶


# ─── aggregate_daily ───

def test_aggregate_month_merge():
    bars = [
        _day(_bj_ms(2026, 7, 1), 10.0, 12.0, 9.5, 11.0, 100),
        _day(_bj_ms(2026, 7, 2), 11.0, 13.5, 10.8, 12.5, 150),
        _day(_bj_ms(2026, 8, 3), 12.5, 14.0, 11.0, 13.0, 200),   # 末桶进行中
    ]
    out = aggregate_daily(bars, "1M")
    assert len(out) == 2
    july, aug = out
    assert july["timestamp"] == _bj_ms(2026, 7, 1)
    assert (july["open"], july["high"], july["low"], july["close"]) == (10.0, 13.5, 9.5, 12.5)
    assert july["volume"] == 250
    assert aug["timestamp"] == _bj_ms(2026, 8, 1)
    assert aug["close"] == 13.0 and aug["volume"] == 200
    assert all("event_ms" in b for b in out)


def test_aggregate_reject_native():
    assert aggregate_daily([_day(_bj_ms(2026, 7, 1), 1, 1, 1, 1, 1)], "1d") == []
    assert aggregate_daily([], "1M") == []


def test_aggregate_intraday_from_1m():
    """1m 基础 K → 10m 派生：OHLC 合并/量求和，末桶进行中照常输出"""
    t0 = _bj_ms(2026, 10, 27)
    bars = [
        _day(t0 + i * 60_000, 100 + i, 105 + i, 95 + i, 101 + i, 10)   # 09:00~09:09 前 10 根
        for i in range(10)
    ]
    bars += [_day(t0 + 10 * 60_000, 200, 210, 190, 205, 7)]            # 第二桶首根（进行中）
    out = aggregate_daily(bars, "10m")
    assert len(out) == 2
    b0, b1 = out
    assert b0["timestamp"] == t0
    assert (b0["open"], b0["high"], b0["low"], b0["close"]) == (100, 114, 95, 110)
    assert b0["volume"] == 100
    assert b1["timestamp"] == t0 + 600_000 and b1["close"] == 205 and b1["volume"] == 7


# ─── base_need / daily_need ───

def test_daily_need():
    assert daily_need("1M", 10) == 10 * 22 * 2
    assert daily_need("1Q", 5) == 5 * 65 * 2
    assert daily_need("1Y", 1000) == FETCH_DAILY_CAP   # 首页封顶
    assert daily_need("2d", 100) == 100 * 2 * 2        # per = round(10/7)+1 = 2
    with pytest.raises(ValueError):
        daily_need("1d", 10)


def test_daily_need_paging():
    """翻页放行：老品种月/季/年线覆盖上市日（如 1991）需远超首页封顶的日 K"""
    assert daily_need("1M", 1000, paging=True) == FETCH_DAILY_CAP * 4   # 宽上限封顶
    assert daily_need("1M", 10, paging=True) == 10 * 22 * 2             # 小需求原样放行


def test_base_need_intraday():
    """日内倍率：每桶 n 根（h 档 60n 根）1m × 2 冗余，受日内封顶"""
    assert base_need("10m", 100) == 100 * 10 * 2
    assert base_need("3h", 10) == 10 * 3 * 60 * 2
    assert base_need("3h", 100) == FETCH_INTRADAY_CAP                  # 需求超封顶被钳制
    assert base_need("2m", 30000) == FETCH_INTRADAY_CAP                 # 首页封顶
    assert base_need("2m", 30000, paging=True) == FETCH_INTRADAY_CAP * 4
    assert daily_need is base_need                                      # 旧名兼容


# ─── fetch_derived_klines（假源） ───

class _FakeSource:
    def __init__(self, daily: list[dict]):
        self._daily = daily
        self.calls: list[tuple] = []
        self.prec: list = []

    async def fetch_klines(self, symbol, timeframe, limit=200, end_time=None):
        self.calls.append((symbol, timeframe, limit, end_time))
        rows = [b for b in self._daily if end_time is None or b["timestamp"] <= end_time]
        return rows[-limit:]

    def _track_prec(self, symbol, values):
        self.prec.append((symbol, list(values)))


async def test_fetch_derived_limit_and_call():
    days = [_day(_bj_ms(2026, 7, d), 10 + d, 12 + d, 9 + d, 11 + d, 10 * d) for d in (1, 2, 3)]
    days += [_day(_bj_ms(2026, 8, d), 20 + d, 22 + d, 19 + d, 21 + d, 20 * d) for d in (3, 4)]
    src = _FakeSource(days)
    out = await fetch_derived_klines(src, "TEST", "1M", limit=1)
    assert len(out) == 1 and out[0]["timestamp"] == _bj_ms(2026, 8, 1)   # 只留末桶
    assert src.calls[0][1] == "1d"                                        # 拉日 K 聚合
    assert src.prec                                                       # 精度批次累积


async def test_fetch_derived_paging_need():
    """带 end_time 翻页时日 K 需求按翻页宽上限放行（不受首页 5000 封顶卡死）"""
    days = [_day(_bj_ms(2026, 8, d), 1, 1, 1, 1, 1) for d in (30, 31)]
    src = _FakeSource(days)
    await fetch_derived_klines(src, "TEST", "1M", limit=1000, end_time=_bj_ms(2026, 8, 31))
    assert src.calls[0][2] == FETCH_DAILY_CAP * 4


async def test_fetch_derived_multi_batch(monkeypatch):
    """首批受封顶不足时自动续拉更早批次（首页月线也能拿全上市至今）"""
    import klinequant.gateway.market_sources.derived as derived_mod
    monkeypatch.setattr(derived_mod, "FETCH_DAILY_CAP", 5)
    days = [_day(_bj_ms(2025, 12, d), 1, 1, 1, 1, 1) for d in range(1, 32)]
    days += [_day(_bj_ms(2026, 1, d), 1, 1, 1, 1, 1) for d in range(1, 32)]
    src = _FakeSource(days)
    out = await fetch_derived_klines(src, "TEST", "1M", limit=2)
    assert [b["timestamp"] for b in out] == [_bj_ms(2025, 12, 1), _bj_ms(2026, 1, 1)]
    assert len(src.calls) >= 2   # 首批受封顶触发了续拉


async def test_fetch_derived_end_time_filter():
    days = [_day(_bj_ms(2026, 7, d), 1, 2, 0.5, 1.5, 10) for d in (1, 2)]
    days += [_day(_bj_ms(2026, 8, 3), 2, 3, 1.5, 2.5, 20)]
    src = _FakeSource(days)
    out = await fetch_derived_klines(src, "TEST", "1M", limit=10, end_time=_bj_ms(2026, 7, 31))
    assert len(out) == 1 and out[0]["timestamp"] == _bj_ms(2026, 7, 1)


# ─── fetch_derived_klines（日内重采样，1m 基础） ───

async def test_fetch_derived_intraday_from_1m():
    """10m 自定义周期：拉 1m 基础 K 聚合，桶标签按 epoch 对齐"""
    t0 = _bj_ms(2026, 10, 27)
    mins = [_day(t0 + i * 60_000, 100 + i, 105 + i, 95 + i, 101 + i, 3) for i in range(25)]
    src = _FakeSource(mins)
    out = await fetch_derived_klines(src, "TEST", "10m", limit=10)
    assert src.calls[0][1] == "1m"                                       # 基础周期 = 1m
    assert [b["timestamp"] for b in out] == [t0, t0 + 600_000, t0 + 1_200_000]
    assert out[0]["volume"] == 30 and out[2]["close"] == 125             # 末桶进行中（5 根）


async def test_fetch_derived_intraday_limit_and_end_time():
    """日内派生同样遵守 limit 截取与 end_time 翻页过滤"""
    t0 = _bj_ms(2026, 10, 27)
    mins = [_day(t0 + i * 60_000, 1, 2, 0.5, 1.5, 1) for i in range(60)]
    src = _FakeSource(mins)
    out = await fetch_derived_klines(src, "TEST", "10m", limit=2)
    assert len(out) == 2 and out[-1]["timestamp"] == t0 + 5 * 600_000    # 只留末两桶
    out2 = await fetch_derived_klines(src, "TEST", "10m", limit=10, end_time=t0 + 2_999_999)
    assert [b["timestamp"] for b in out2] == [t0, t0 + 600_000, t0 + 1_200_000,
                                              t0 + 1_800_000, t0 + 2_400_000]


# ─── base_fetch 注入（缓存层共享基础 K 存量；缺省直连源插件） ───

async def test_fetch_derived_uses_injected_base_fetch():
    """注入 base_fetch 时基础 K 取数走注入器（生产为 cached_klines），不再直连源插件

    回归锁：派生周期基础 K 必须可被缓存层接管，否则每个派生请求各自冷拉基础 K，
    公网源经代理串行下 K 线路由 + 指标预热重复冷拉致指标突破前端 30s 超时不显示。
    """
    t0 = _bj_ms(2026, 10, 27)
    mins = [_day(t0 + i * 60_000, 100 + i, 105 + i, 95 + i, 101 + i, 3) for i in range(25)]
    src = _FakeSource(mins)
    seen: list[tuple] = []

    async def spy_base(source, symbol, base_tf, limit, end_time):
        seen.append((source is src, symbol, base_tf, limit, end_time))
        return await source.fetch_klines(symbol, base_tf, limit=limit, end_time=end_time)

    out = await fetch_derived_klines(src, "TEST", "10m", limit=10, base_fetch=spy_base)
    assert seen and all(s[0] for s in seen)          # 注入器被调用且透传 source
    assert seen[0][2] == "1m"                        # 基础周期 = 1m（日内重采样）
    assert [b["timestamp"] for b in out] == [t0, t0 + 600_000, t0 + 1_200_000]   # 聚合结果与直连一致


async def test_fetch_derived_default_base_fetch_is_direct():
    """缺省不注入 base_fetch 时直连源插件 fetch_klines（单测/无缓存上下文兜底路径）"""
    t0 = _bj_ms(2026, 10, 27)
    mins = [_day(t0 + i * 60_000, 1, 2, 0.5, 1.5, 1) for i in range(25)]
    src = _FakeSource(mins)
    out = await fetch_derived_klines(src, "TEST", "10m", limit=5)
    assert src.calls and src.calls[0][1] == "1m"     # 直连源插件拉 1m 基础 K
    assert len(out) == 3
