"""网关级派生周期层（周/月/季/年/自定义，含分钟·小时级重采样）

各数据源原生周期最粗普遍只到周线（同花顺仅 1m~week；币安/MT5 映射表同止于 1w），
月/季/年/自定义倍率周期无源可取，统一由网关从基础周期聚合（唯一口径，不接个别源
原生月线避免双口径）；1w 周线各源原生直供不走派生。

基础周期两级：日级及以上（d/w/M/Q/Y）从 1d 聚合；分钟/小时级自定义倍率从 1m
重采样（各源均有 1m 原生供给）；源原生支持的档（含 2h/4h 等）直接透传不派生，
仅当源不供给该档时才走重采样（resolve_tf 按源判定，如 ths 无 4h → 由 1m 派生）。

桶标签 = 开盘时刻：月=当月 1 日、季=季度首日、年=1 月 1 日（北京时间）；
Nd 以 1970-01-01、Nw 以 1969-12-29（周一）为对齐起点；分钟/小时级按 epoch
整除对齐（UTC，同币安/TV 口径，不随交易时段切分）。复权随基础 K（如 ths 前复权）。

周期编码：固定档 1M/1Q/1Y + 自定义 `数字+单位`（2~99 倍率，单位 m/h/d/w/M）。
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone

_BJ = timezone(timedelta(hours=8))   # A 股无夏令时，固定 +8

#: 固定派生档位（月/季/年无算术通式，月线亦走派生口径）
DERIVED_FIXED = ("1M", "1Q", "1Y")

#: 原生周期（全源均直供、永不走派生的档；小时档 2h/4h/6h/12h 仅部分源直供，
#: 不在此列——格式上按 Nh 倍率可解析，源原生支持时由 resolve_tf 透传，
#: 源缺供时（如 ths 无 4h）走 1m 重采样兜底）
NATIVE_TIMEFRAMES = frozenset({
    "1m", "3m", "5m", "15m", "30m",
    "1h", "1d", "3d", "1w",
})

_CUSTOM_RE = re.compile(r"^(\d{1,2})([mhdwM])$")

#: 日内基础周期（分钟/小时级自定义倍率的重采样基底）
_INTRADAY_UNITS = frozenset({"m", "h"})

#: 每桶交易日数估算（日 K 拉取量换算基准，实际拉取另乘 2 倍冗余）
_DAYS_PER_BUCKET = {"1M": 22, "1Q": 65, "1Y": 250}

#: 单次派生拉取的日 K 上限（年线 1000 根理论需 25 万根，封顶防滥用）
FETCH_DAILY_CAP = 5000

#: 单次派生拉取的日内基础 K（1m）上限（≈14 个交易日连续分钟数，封顶防滥用）
FETCH_INTRADAY_CAP = 20000

_EPOCH = datetime(1970, 1, 1, tzinfo=_BJ)
_MONDAY_ANCHOR = datetime(1969, 12, 29, tzinfo=_BJ)   # epoch 前最近周一（周对齐起点）


def parse_tf(tf: str) -> tuple[int, str] | None:
    """派生周期解析 → (倍率, 单位 m/h/d/w/M/Q/Y)；原生周期与非法格式返回 None

    自定义倍率格式为 `数字+单位`；n=1 且非固定档的形态（1m/1h/1d/1w…）均属
    NATIVE_TIMEFRAMES 先行透传，不会进入倍率解析。注意格式可派生 ≠ 实际走派生，
    按源路由用 resolve_tf。
    """
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
    """格式上是否为网关派生周期（缓存/指标链路的统一判定入口；按源路由用 resolve_tf）"""
    return parse_tf(tf) is not None


def base_tf_for(tf: str) -> str:
    """派生周期的聚合基础周期：分钟/小时级 → 1m 重采样，日级及以上 → 1d"""
    parsed = parse_tf(tf)
    if parsed is None:
        raise ValueError(f"not a derived timeframe: {tf}")
    return "1m" if parsed[1] in _INTRADAY_UNITS else "1d"


def resolve_tf(tf: str, supported: set[str] | frozenset[str]) -> str | None:
    """按源路由判定：返回聚合基础周期（走派生）或 None（源原生直供/非法档透传拒绝）

    - 源原生支持（如币安 2h/4h、各源 1m/1w）：透传，不双重口径；
    - 源不支持且格式可派生（如 ths 4h、任意源 2m/10m/2d/1M）：走派生聚合；
    - 基础档自身（1m）源都不供时无基底可聚合，返 None 由路由层拒绝。
    """
    if tf in supported:
        return None
    if tf == "1m":
        return None
    if parse_tf(tf) is None:
        return None
    return base_tf_for(tf)


def bucket_label(day_ts_ms: int, parsed: tuple[int, str]) -> int:
    """基础 K 时间戳 → 所属派生桶的开盘时刻（epoch 毫秒）

    日级及以上按北京时间日历对齐；分钟/小时级按 epoch 整除对齐（UTC，
    同币安/TV 自定义周期口径，不随交易时段切分——A 股开盘落在桶中段属预期）。
    """
    n, unit = parsed
    if unit == "m":
        span = n * 60_000
        return day_ts_ms // span * span
    if unit == "h":
        span = n * 3_600_000
        return day_ts_ms // span * span
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
    """基础 K 序列（升序，日 K 或 1m）→ 派生周期 bar（OHLC 合并/量求和，末桶进行中照常输出）"""
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


def base_need(tf: str, limit: int, paging: bool = False) -> int:
    """派生周期 limit 根所需基础 K 拉取量（每桶基础根数估算 × 2 冗余）

    首页受封顶（日级 FETCH_DAILY_CAP / 日内 FETCH_INTRADAY_CAP）防滥用；
    翻页（paging）按调用方请求量放行——老品种月/季/年线要覆盖上市日（如 1991 年），
    日 K 需求远超 5000，翻页本身已受前端 PAGE_SIZE 约束无滥用风险。
    """
    parsed = parse_tf(tf)
    if parsed is None:
        raise ValueError(f"not a derived timeframe: {tf}")
    n, unit = parsed
    if unit == "m":
        need = limit * n * 2
        cap = FETCH_INTRADAY_CAP
    elif unit == "h":
        # 每桶 n 小时：加密 24h 连续市最多 60n 根 1m；×2 冗余对 A 股（每日仅 4h）同时是深度补偿
        need = limit * n * 60 * 2
        cap = FETCH_INTRADAY_CAP
    else:
        per = _DAYS_PER_BUCKET.get(tf)
        if per is None:
            if unit == "d":
                per = max(1, round(n * 5 / 7)) + 1
            elif unit == "w":
                per = n * 5 + 1
            else:   # NM 月倍率
                per = n * 22
        need = limit * per * 2
        cap = FETCH_DAILY_CAP
    # 翻页亦设宽上限（A 股全史 ≈ 9000 交易日，×2 冗余足够），避免无界拉取
    return min(need, cap * 4) if paging else min(need, cap)


#: 兼容旧名（日级派生的基础 K 需求量，语义同 base_need）
daily_need = base_need


async def _fetch_base_direct(source, symbol: str, base_tf: str, limit: int, end_time: int | None):
    """基础周期默认取数器：直连源插件（不经进程缓存）。

    仅在 fetch_derived_klines 被独立调用（单测/无缓存上下文）时兜底；生产路径由
    kline_cache._fetch_page 注入 cached_klines 作为 base_fetch，令基础 K 与 REST 行情、
    指标预热共享存量（否则每个派生请求都冷拉一遍基础 K，公网源上极慢）。
    """
    return await source.fetch_klines(symbol, base_tf, limit=limit, end_time=end_time)


async def fetch_derived_klines(
    source, symbol: str, tf: str, limit: int, end_time: int | None = None,
    *, base_fetch=None,
) -> list[dict]:
    """派生周期历史 K 线：拉基础周期聚合（/klines 路由与指标预热共用入口）

    基础周期由 base_tf_for 决定（日级 1d / 日内 1m）。end_time 翻页语义不变
    （对基础 K 区间查询后聚合，桶标签 <= end_time 过滤）。
    基础 K 单批受封顶约束时内部自动续拉更早批次，直到凑满 limit 根或数据源见底，
    保证老品种月/季/年线一次请求即覆盖上市日（前端 noMoreData 语义不被首批封顶误导）。

    base_fetch：基础 K 取数器，签名 (source, symbol, base_tf, limit, end_time) -> bars。
    缓存层（kline_cache）注入 cached_klines，使基础 K 走进程级缓存并被 REST 行情与
    多个指标预热共享——同一份基础 K 聚合出的桶数通常远超单次显示需求（如 20000 根 1m
    可聚 ~238 个 84m 桶），故 K 线路由拉过一次后，指标预热加深即可零网络命中，
    消除「派生周期指标各自冷拉基础 K 致 30s 超时不显示」。缺省 None 时直连源插件。
    """
    fetch_base = base_fetch or _fetch_base_direct
    base_tf = base_tf_for(tf)
    intraday = base_tf == "1m"
    dailies: list[dict] = []
    cursor = end_time
    for _ in range(8):   # 批次上限防御（宽上限下 A 股全史两批内到底）
        # 单批需求：首页受对应封顶，翻页放宽至 4 倍封顶（cap 读模块级常量，测试可 monkeypatch）
        cap = FETCH_INTRADAY_CAP if intraday else FETCH_DAILY_CAP
        need = min(base_need(tf, limit, paging=True), cap * 4) if cursor is not None \
            else min(base_need(tf, limit), cap)
        page = await fetch_base(source, symbol, base_tf, need, cursor)
        if not page:
            break
        dailies = page + dailies
        if len(aggregate_daily(dailies, tf)) >= limit:
            break
        if len(page) < need:   # 未凑满但源已到底（返回量不足请求量）
            break
        cursor = min(int(b["timestamp"]) for b in page) - 1
    out = aggregate_daily(dailies, tf)
    if end_time:
        out = [b for b in out if b["timestamp"] <= end_time]
    # 价格精度沿用源侧推导（基础 K 价格批次累积，前端只渲染）
    for b in dailies[-20:]:
        source._track_prec(symbol, [b["open"], b["high"], b["low"], b["close"]])
    return out[-limit:]
