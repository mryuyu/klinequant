"""网关级派生周期层（周/月/季/年/自定义）

各数据源原生周期最粗普遍只到周线（同花顺仅 1m~week；币安/MT5 映射表同止于 1w），
月/季/年/自定义倍率周期无源可取，统一由网关从日 K 聚合（唯一口径，不接个别源
原生月线避免双口径）；1w 周线各源原生直供不走派生。

桶标签 = 开盘时刻（北京时间）：月=当月 1 日、季=季度首日、年=1 月 1 日；
Nd 以 1970-01-01、Nw 以 1969-12-29（周一）为对齐起点。复权随日 K（如 ths 前复权）。

周期编码：固定档 1M/1Q/1Y + 自定义 `数字+单位`（2~99 倍率，单位仅 d/w/M；
分钟/小时级原生档已覆盖，不做重采样派生）。
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone

_BJ = timezone(timedelta(hours=8))   # A 股无夏令时，固定 +8

#: 固定派生档位（月/季/年无算术通式，月线亦走派生口径）
DERIVED_FIXED = ("1M", "1Q", "1Y")

#: 原生周期（各源可能直供，派生解析须排除直接透传）
NATIVE_TIMEFRAMES = frozenset({
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "12h",
    "1d", "3d", "1w",
})

_CUSTOM_RE = re.compile(r"^(\d{1,2})(d|w|M)$")

#: 每桶交易日数估算（日 K 拉取量换算基准，实际拉取另乘 2 倍冗余）
_DAYS_PER_BUCKET = {"1M": 22, "1Q": 65, "1Y": 250}

#: 单次派生拉取的日 K 上限（年线 1000 根理论需 25 万根，封顶防滥用）
FETCH_DAILY_CAP = 5000

_EPOCH = datetime(1970, 1, 1, tzinfo=_BJ)
_MONDAY_ANCHOR = datetime(1969, 12, 29, tzinfo=_BJ)   # epoch 前最近周一（周对齐起点）


def parse_tf(tf: str) -> tuple[int, str] | None:
    """派生周期解析 → (倍率, 单位 d/w/M/Q/Y)；原生周期与非法格式返回 None"""
    if tf == "1Q":
        return (1, "Q")
    if tf == "1Y":
        return (1, "Y")
    if tf in NATIVE_TIMEFRAMES:
        return None
    m = _CUSTOM_RE.match(tf or "")
    if not m:
        return None
    n = int(m.group(1))
    if not 1 <= n <= 99:
        return None
    return (n, m.group(2))


def is_derived(tf: str) -> bool:
    """是否为网关派生周期（路由/订阅/指标链路的统一判定入口）"""
    return parse_tf(tf) is not None


def bucket_label(day_ts_ms: int, parsed: tuple[int, str]) -> int:
    """日 K 时间戳 → 所属派生桶的开盘时刻（epoch 毫秒，北京时间）"""
    n, unit = parsed
    dt = datetime.fromtimestamp(day_ts_ms / 1000, tz=_BJ)
    if unit == "M":
        months = (dt.year - 1970) * 12 + (dt.month - 1)
        base = months // n * n
        y, m0 = divmod(base, 12)
        label = datetime(1970 + y, m0 + 1, 1, tzinfo=_BJ)
    elif unit == "Q":
        q = (dt.year - 1970) * 4 + (dt.month - 1) // 3
        base = q // n * n
        y, q0 = divmod(base, 4)
        label = datetime(1970 + y, q0 * 3 + 1, 1, tzinfo=_BJ)
    elif unit == "Y":
        label = datetime(1970 + (dt.year - 1970) // n * n, 1, 1, tzinfo=_BJ)
    elif unit == "w":
        days = (dt - _MONDAY_ANCHOR).days
        label = _MONDAY_ANCHOR + timedelta(days=days // (7 * n) * (7 * n))
    else:   # d
        days = (dt - _EPOCH).days
        label = _EPOCH + timedelta(days=days // n * n)
    return int(label.timestamp() * 1000)


def aggregate_daily(bars: list[dict], tf: str) -> list[dict]:
    """日 K 序列（升序）→ 派生周期 bar（OHLC 合并/量求和，末桶进行中照常输出）"""
    parsed = parse_tf(tf)
    if parsed is None or not bars:
        return []
    out: list[dict] = []
    cur: dict | None = None
    cur_label = None
    for b in bars:
        label = bucket_label(int(b["timestamp"]), parsed)
        if label != cur_label:
            if cur is not None:
                out.append(cur)
            cur_label = label
            cur = {
                "timestamp": label,
                "open": float(b["open"]), "high": float(b["high"]),
                "low": float(b["low"]), "close": float(b["close"]),
                "volume": float(b.get("volume") or 0),
            }
        else:
            cur["high"] = max(cur["high"], float(b["high"]))
            cur["low"] = min(cur["low"], float(b["low"]))
            cur["close"] = float(b["close"])
            cur["volume"] += float(b.get("volume") or 0)
    if cur is not None:
        out.append(cur)
    now_ms = int(time.time() * 1000)
    for bar in out:
        bar["event_ms"] = now_ms
    return out


def daily_need(tf: str, limit: int) -> int:
    """派生周期 limit 根所需日 K 拉取量（每桶交易日估算 × 2 冗余，封顶防滥用）"""
    parsed = parse_tf(tf)
    if parsed is None:
        raise ValueError(f"not a derived timeframe: {tf}")
    n, unit = parsed
    per = _DAYS_PER_BUCKET.get(tf)
    if per is None:
        if unit == "d":
            per = max(1, round(n * 5 / 7)) + 1
        elif unit == "w":
            per = n * 5 + 1
        else:   # NM 月倍率
            per = n * 22
    return min(limit * per * 2, FETCH_DAILY_CAP)


async def fetch_derived_klines(
    source, symbol: str, tf: str, limit: int, end_time: int | None = None,
) -> list[dict]:
    """派生周期历史 K 线：拉日 K 聚合（/klines 路由与指标预热共用入口）

    end_time 翻页语义不变（对日 K 区间查询后聚合，桶标签 <= end_time 过滤）。
    """
    need = daily_need(tf, limit)
    bars = await source.fetch_klines(symbol, "1d", limit=need, end_time=end_time)
    out = aggregate_daily(bars or [], tf)
    if end_time:
        out = [b for b in out if b["timestamp"] <= end_time]
    # 价格精度沿用源侧推导（日 K 价格批次累积，前端只渲染）
    for b in (bars or [])[-20:]:
        source._track_prec(symbol, [b["open"], b["high"], b["low"], b["close"]])
    return out[-limit:]
