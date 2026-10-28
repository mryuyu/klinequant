"""进程级 K 线缓存（同品种同周期来回切换免重复拉取/聚合）

设计：
    - 键 = (市场源, 品种, 周期)，值为按时间升序的 bar 列表 + 是否已到源尽头标记；
      原生与派生周期同层缓存（派生命中后免重拉日 K 与聚合）。
    - 命中：直接按 end_time/limit 切片；仅当请求窗口触及最新端时做一次小额尾部刷新
      （拉最新几根合并），保证未收盘 bar 不陈旧。
    - 缺口补拉：请求深度超过已缓存最旧根时，以缓存最旧根为锚点向前补页，
      直至凑满或源尽头；补拉异常时保留已有数据退回（不标尽头，源恢复后可继续）。
    - LRU：条目上限 _MAX_ENTRIES，超限淘汰最久未用。
    - 并发：按键加锁，防同键并发深拉踩踏。
进程重启即失效（设计如此，不落库）。
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import OrderedDict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_MAX_ENTRIES = 64       # 缓存条目上限（约 64 × 3 万根 × ~150B ≈ 300MB 上限，实际远达不到）
_TAIL_REFRESH = 3       # 命中且窗口含最新端时的尾部刷新根数（覆盖未收盘 bar）
_TAIL_REFRESH_TTL = 2.0  # 尾刷最小间隔（秒）：切换 burst 内同键并发（REST K 线 + N 个指标
                         # 预热）串行命中，各自尾刷一次（公网源经代理 ~0.7s/往返）冗余拖慢
                         # 冷跨市场首屏；TTL 内复用现有 bars 跳过重复尾刷（未收盘 bar 至多
                         # TTL 秒陈旧，由 WS 实时推送纠正；REST 仅用于历史/初载，低频）

# 内部中空洞自愈：源掉线/子进程重启期间实时流漏采的 bar，会在缓存中段留下永久
# 缺口——尾部刷新只取最新 3 根、前向补拉只朝更早方向，两者都无法回填中段。表现为
# K 线缺口处蜡烛图空缺、而指标线跨缺口相连（line series 直连相邻点）。命中最新时
# 扫描服务窗口内的可疑空洞并向源补拉；源确无数据的合法空洞（周末/休市）返空后按
# 冷却跳过，不反复拉取。仅对日内原生档（<1 日）生效：日/周线的周末缺口属正常结构。
_GAP_TOL = 1.5                 # 相邻间隔 > interval×该系数即判缺洞（≥1 根缺失）
_GAP_REPAIR_MAX = 3            # 单次请求最多补拉的缺洞数
_GAP_RETRY_COOLDOWN = 120.0    # 同一缺洞补拉返空后的重试冷却（秒）
_GAP_FETCH_MARGIN = 4          # 补拉范围按缺失根数留的冗余
_DAY_MS = 86_400_000
_TF_RE = re.compile(r"^(\d+)([mhdw])$")
_TF_UNIT_MS = {"m": 60_000, "h": 3_600_000, "d": _DAY_MS, "w": 604_800_000}

# 短页≠尽头的源（history_end_on_short_page=False，如 MT5 终端源）的尽头标记冷却：
# 冷却内视同尽头不重复补拉，过期后允许重试——终端侧渐进同步/手动加深历史后，
# 免重启进程即可续拉（永久焊死实证：XAUUSD 1m 卡 1038 根、EURUSD 1m 卡 1056 根）。
_EXHAUST_RETRY_COOLDOWN = 120.0
# 单次请求补拉轮数护栏（50×8000 根 ≫ 路由 limit 上限 30000）：防极端缺口源
# 单请求内无限翻页；未凑满不标尽头，下次请求可继续。
_FILL_MAX_ROUNDS = 50


@dataclass
class _Entry:
    bars: list[dict] = field(default_factory=list)   # 时间升序
    exhausted: bool = False                          # 源已无更早数据
    exhausted_at: float = 0.0                        # 尽头标记时刻(monotonic)，冷却过期可重试
    gap_attempts: dict = field(default_factory=dict)  # (a_ts,b_ts) -> monotonic，缺洞补拉冷却台账
    last_tail_at: float = 0.0                        # 上次尾刷时刻(monotonic)；TTL 内跳过重复尾刷


_cache: OrderedDict[tuple, _Entry] = OrderedDict()
_locks: dict[tuple, asyncio.Lock] = {}


def _entry(key: tuple) -> _Entry:
    e = _cache.get(key)
    if e is None:
        e = _Entry()
        _cache[key] = e
    _cache.move_to_end(key)
    while len(_cache) > _MAX_ENTRIES:
        _cache.popitem(last=False)
    return e


def _lock(key: tuple) -> asyncio.Lock:
    lk = _locks.get(key)
    if lk is None:
        lk = asyncio.Lock()
        _locks[key] = lk
    return lk


async def _fetch_page(source, symbol: str, tf: str, limit: int, end_time=None) -> list[dict]:
    """单页拉取：派生周期走聚合入口，原生直连源插件

    派生判定按源（同路由 resolve_tf 口径）而非纯格式：格式可解析但源原生
    直供的档（如币安 2h/4h）必须透传，否则与 REST 路由双口径不一致；
    未声明 supported_timeframes 的源（测试桩）按全局原生档集透传。
    """
    from klinequant.gateway.market_sources.derived import (
        NATIVE_TIMEFRAMES,
        fetch_derived_klines,
        parse_tf,
    )
    native = getattr(source, "supported_timeframes", None) or NATIVE_TIMEFRAMES
    if tf not in native and tf != "1m" and parse_tf(tf) is not None:
        return await fetch_derived_klines(source, symbol, tf, limit=limit, end_time=end_time)
    return await source.fetch_klines(symbol, tf, limit=limit, end_time=end_time)


def _merge_front(entry: _Entry, page: list[dict]) -> None:
    """旧页前插合并（按时间戳去重，页内升序）"""
    have = {int(b["timestamp"]) for b in entry.bars}
    front = [b for b in page if int(b["timestamp"]) not in have]
    entry.bars = sorted(front + entry.bars, key=lambda b: int(b["timestamp"]))


def _exhausted_active(source, e: _Entry) -> bool:
    """尽头标记是否仍生效（补拉循环短路条件）

    REST 源（history_end_on_short_page 默认 True）短页即尽头，永久生效；
    MT5 类终端源短页可能只是终端历史尚未同步完，冷却过期后放行重试补拉。
    """
    if not e.exhausted:
        return False
    if getattr(source, "history_end_on_short_page", True):
        return True
    return time.monotonic() - e.exhausted_at < _EXHAUST_RETRY_COOLDOWN


def _merge_tail(entry: _Entry, page: list[dict]) -> None:
    """尾部刷新合并：同时间戳覆盖（未收盘 bar 更新），新根追加"""
    idx = {int(b["timestamp"]): i for i, b in enumerate(entry.bars)}
    for b in page:
        i = idx.get(int(b["timestamp"]))
        if i is not None:
            entry.bars[i] = b
        else:
            entry.bars.append(b)
            idx[int(b["timestamp"])] = len(entry.bars) - 1
    entry.bars.sort(key=lambda b: int(b["timestamp"]))


def _filter_subinterval(bars: list[dict], interval_ms: int | None) -> list[dict]:
    """v9.170 串周期污染防火墙：剔除与前一根保留 bar
    间距 <0.9×interval 的 bar（混入的他周期/重复 bar）

    口径用间距而非网格对齐：部分源（MT5）高周期 bar 时间戳带服务器时区偏移网格
    （实测 4h 档 ts%4h≡1h），对齐会误杀；合法缺口（周末/DST）只产生更大间距、永不更小。
    派生/月季年档 interval_ms=None 不过滤（无稳定间距口径）。
    """
    if not interval_ms or not bars:
        return bars
    thr = interval_ms * 0.9
    out: list[dict] = []
    last = None
    for b in bars:
        ts = int(b["timestamp"])
        if last is None or ts - last >= thr:
            out.append(b)
            last = ts
    # 多数护栏：单遍剔除过半 ⇒ 序列自身即为子间隔（周期假设不成立/整页污染）不过滤——
    # 「抽稀」会产出间距均匀但 OHLC 错误的假 bar，不如保留可见破绽（合成时间戳单测亦由此豁免）
    if (len(bars) - len(out)) * 2 >= len(bars):
        return bars
    if len(out) < len(bars):
        logger.warning(
            f"kline sanitize dropped {len(bars) - len(out)} sub-interval bars (iv={interval_ms}ms)"
        )
    return out


def _interval_ms(timeframe: str, source=None) -> int | None:
    """周期 → 单根跨度(ms)。实际走派生聚合的档返回 None（聚合窗口语义由派生层自管）；
    源原生直供的档（含格式可解析的 2h/4h 等，如币安）按原生跨度参与缺口自愈/尽头判定。"""
    from klinequant.gateway.market_sources.derived import NATIVE_TIMEFRAMES, parse_tf
    native = getattr(source, "supported_timeframes", None) or NATIVE_TIMEFRAMES
    if timeframe not in native and timeframe != "1m" and parse_tf(timeframe) is not None:
        return None
    m = _TF_RE.match(timeframe or "")
    return int(m.group(1)) * _TF_UNIT_MS[m.group(2)] if m else None


async def _repair_interior_gaps(
    source, symbol: str, timeframe: str, e: _Entry,
    interval_ms: int, end_time: int | None, limit: int,
) -> None:
    """补拉服务窗口内的中空洞（源掉线遗留、尾刷/前补都无法修复的缺口）。

    仅扫描将返回的窗口（末尾 limit 根、≤end_time）；每个缺洞冷却内只试一次，源返空
    （合法休市/周末）则记录冷却不反复拉取；拉到数据则按时间戳去重合入缓存。
    """
    hi_idx = len(e.bars) if end_time is None else _count_le(e.bars, end_time)
    lo_idx = max(0, hi_idx - limit)
    if hi_idx - lo_idx < 2:
        return
    thr = interval_ms * _GAP_TOL
    gaps = []
    for i in range(lo_idx, hi_idx - 1):          # 仅扫描将返回的服务窗口，免深缓存全量 O(n)
        a, b = int(e.bars[i]["timestamp"]), int(e.bars[i + 1]["timestamp"])
        if b - a > thr:
            gaps.append((a, b))
    if not gaps:
        return
    if len(e.gap_attempts) > 512:            # 台账兜底防膨胀
        e.gap_attempts.clear()
    now = time.monotonic()
    done = 0
    for a, b in gaps:
        if done >= _GAP_REPAIR_MAX:
            break
        prev = e.gap_attempts.get((a, b))
        if prev is not None and now - prev < _GAP_RETRY_COOLDOWN:
            continue
        e.gap_attempts[(a, b)] = now
        done += 1
        need = min((b - a) // interval_ms + _GAP_FETCH_MARGIN, 8000)
        try:
            page = await _fetch_page(source, symbol, timeframe, need, b - 1)
        except Exception as ex:
            logger.warning(f"kline gap repair failed [{source.name}] {symbol}/{timeframe}: {ex}")
            continue
        mid = [x for x in page if a < int(x["timestamp"]) < b] if page else []
        if mid:
            _merge_front(e, mid)
            logger.info(f"gap repaired [{source.name}] {symbol}/{timeframe}: +{len(mid)} bars")


async def cached_klines(
    source,
    symbol: str,
    timeframe: str,
    limit: int,
    end_time: int | None = None,
) -> list[dict]:
    """带进程缓存的 K 线拉取（语义与源插件 fetch_klines 一致：返回 ≤end_time 的最新 limit 根）"""
    key = (source.name, symbol, timeframe)
    async with _lock(key):
        e = _entry(key)

        # 窗口含最新端时做尾部小额刷新（未收盘 bar 不陈旧）；首拉空缓存也走此路顺带建底。
        # 注意：exhausted 只表示“无更早数据”，最新端仍会变，不受它影响。
        touches_latest = end_time is None or (
            bool(e.bars) and end_time >= int(e.bars[-1]["timestamp"])
        )
        # 尾刷 TTL：已有存量且距上次尾刷 < TTL 时跳过（冷启动无存量必拉，不受限）。
        # 收敛切换 burst 内 REST K 线 + 多个指标预热对同键的重复公网尾刷往返。
        skip_tail = bool(e.bars) and (time.monotonic() - e.last_tail_at) < _TAIL_REFRESH_TTL
        if touches_latest and not skip_tail:
            tail_n = _TAIL_REFRESH if e.bars else limit
            try:
                tail = await _fetch_page(source, symbol, timeframe, tail_n, None)
            except Exception:
                if not e.bars:
                    raise   # 首拉失败无存量兜底，向上抛由路由兜底
                tail = []   # 有存量：刷新失败退回缓存（可能陈旧一根）
            e.last_tail_at = time.monotonic()
            if tail:
                if e.bars:
                    _merge_tail(e, tail)
                else:
                    e.bars = sorted(tail, key=lambda b: int(b["timestamp"]))
                # 不据短返回推定尽头：部分源裸拉有内部单次上限（如 thsdk ~6000），
                # 短返回可能是截断而非全量；尽头只由补拉路径的空页/短页判定

        # 缺口补拉：请求深度超过已缓存最旧根
        interval_ms = _interval_ms(timeframe, source)
        # 短页==尽头：REST 源恒成立（单次上限固定，短页=回溯到上市日）；终端源（MT5）
        # 原生档不成立——短页多为补拉窗口内的周末/休市空洞（实证：黄金 M1 窗口跨周末
        # 只返 748/1000 根被误判尽头，缓存永远停在本周开盘处），须继续向前翻页，
        # 空页才判尽头；派生档（interval_ms=None）窗口语义由聚合层自管，仍按短页即尽头。
        end_on_short = getattr(source, "history_end_on_short_page", True) or interval_ms is None
        need = limit - _count_le(e.bars, end_time)
        anchor = (int(e.bars[0]["timestamp"]) - 1) if e.bars else end_time
        rounds = 0
        while need > 0 and not _exhausted_active(source, e):
            rounds += 1
            if rounds > _FILL_MAX_ROUNDS:
                break   # 护栏：不标尽头，下次请求可继续补拉
            take = min(max(need, 1000), 8000)
            try:
                page = await _fetch_page(source, symbol, timeframe, take, anchor)
            except Exception as ex:
                logger.warning(
                    f"kline cache fill failed [{source.name}] {symbol}/{timeframe}: {ex}"
                )
                break   # 保留已有数据退回；不标尽头，源恢复后下次请求可继续补拉
            if not page:
                e.exhausted = True
                e.exhausted_at = time.monotonic()
                break
            _merge_front(e, page)
            e.exhausted = False   # 补拉有数据即解除旧尽头标记，按本页返回量重判
            if len(page) < take and end_on_short:
                e.exhausted = True
                e.exhausted_at = time.monotonic()
                break
            # 终端源短页不中断：以本页最旧根为新锚点继续向前翻页
            # （page[0] ≤ anchor ⇒ 锚点严格递减，无死循环；越过空洞后仍可拿到更早历史）
            anchor = int(page[0]["timestamp"]) - 1
            need = limit - _count_le(e.bars, end_time)

        #  内部中空洞自愈（仅日内原生档）：源掉线遗留的缺口尾刷/前补都修不了
        if interval_ms and interval_ms < _DAY_MS and len(e.bars) >= 2:
            try:
                await _repair_interior_gaps(
                    source, symbol, timeframe, e, interval_ms, end_time, limit
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                logger.warning(f"gap repair skipped [{source.name}] {symbol}/{timeframe}: {ex}")

        # v9.170 串周期污染防火墙：尾刷/前补/补洞合入后统一收口剔除子间隔 bar
        # （顺带愈合存量污染缓存）
        e.bars = _filter_subinterval(e.bars, interval_ms)
        out = [b for b in e.bars if end_time is None or int(b["timestamp"]) <= end_time]
        return out[-limit:]


def _count_le(bars: list[dict], end_time: int | None) -> int:
    if end_time is None:
        return len(bars)
    lo, hi = 0, len(bars)
    while lo < hi:
        mid = (lo + hi) // 2
        if int(bars[mid]["timestamp"]) <= end_time:
            lo = mid + 1
        else:
            hi = mid
    return lo


def peek_timestamps(source, symbol: str, timeframe: str, tail: int) -> list[int]:
    """只读 peek 内存缓存尾部 tail 根 timestamp（不拉取/不尾刷/不补拉）；缓存未命中返空

    供指标引擎漏 bar 自检对拍用（ensure_warmed 短路分支）：短路路径必须零源请求，
    不能走 cached_klines（其尾刷/补拉本身即 fetch）。
    """
    e = _cache.get((source.name, symbol, timeframe))
    if e is None or not e.bars:
        return []
    return [int(b["timestamp"]) for b in e.bars[-tail:]]


def clear() -> None:
    """清空缓存（测试/调试用）"""
    _cache.clear()
    _locks.clear()
