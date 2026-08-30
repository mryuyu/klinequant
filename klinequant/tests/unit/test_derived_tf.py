"""网关派生周期层（derived）单元测试

覆盖：
    - parse_tf：固定档/自定义倍率解析与非法格式拒绝
    - bucket_label：月/季/年/周倍率/日倍率桶标签（北京时区、跨边界）
    - aggregate_daily：OHLC 合并/量求和/末桶进行中输出
    - daily_need：日 K 拉取量换算与封顶
    - fetch_derived_klines：假源拉日 K 聚合、limit 截取、end_time 过滤
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gateway.market_sources.derived import (
    FETCH_DAILY_CAP,
    aggregate_daily,
    bucket_label,
    daily_need,
    fetch_derived_klines,
    is_derived,
    parse_tf,
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


@pytest.mark.parametrize("tf", ["3h", "100M", "0d", "1d", "1w", "15m", "abc", "", "2q", "1.5d"])
def test_parse_tf_reject(tf):
    assert parse_tf(tf) is None
    assert not is_derived(tf)


def test_is_derived_true():
    for tf in ("1M", "1Q", "1Y", "2d", "3w", "4M"):
        assert is_derived(tf)


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


# ─── daily_need ───

def test_daily_need():
    assert daily_need("1M", 10) == 10 * 22 * 2
    assert daily_need("1Q", 5) == 5 * 65 * 2
    assert daily_need("1Y", 1000) == FETCH_DAILY_CAP   # 封顶
    assert daily_need("2d", 100) == 100 * 2 * 2        # per = round(10/7)+1 = 2
    with pytest.raises(ValueError):
        daily_need("1d", 10)


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


async def test_fetch_derived_end_time_filter():
    days = [_day(_bj_ms(2026, 7, d), 1, 2, 0.5, 1.5, 10) for d in (1, 2)]
    days += [_day(_bj_ms(2026, 8, 3), 2, 3, 1.5, 2.5, 20)]
    src = _FakeSource(days)
    out = await fetch_derived_klines(src, "TEST", "1M", limit=10, end_time=_bj_ms(2026, 7, 31))
    assert len(out) == 1 and out[0]["timestamp"] == _bj_ms(2026, 7, 1)
