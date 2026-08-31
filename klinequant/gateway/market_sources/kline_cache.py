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
from collections import OrderedDict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_MAX_ENTRIES = 64       # 缓存条目上限（约 64 × 3 万根 × ~150B ≈ 300MB 上限，实际远达不到）
_TAIL_REFRESH = 3       # 命中且窗口含最新端时的尾部刷新根数（覆盖未收盘 bar）


@dataclass
class _Entry:
    bars: list[dict] = field(default_factory=list)   # 时间升序
    exhausted: bool = False                          # 源已无更早数据


_cache: "OrderedDict[tuple, _Entry]" = OrderedDict()
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
    """单页拉取：派生周期走聚合入口，原生直连源插件"""
    from gateway.market_sources.derived import fetch_derived_klines, is_derived
    if is_derived(tf):
        return await fetch_derived_klines(source, symbol, tf, limit=limit, end_time=end_time)
    return await source.fetch_klines(symbol, tf, limit=limit, end_time=end_time)


def _merge_front(entry: _Entry, page: list[dict]) -> None:
    """旧页前插合并（按时间戳去重，页内升序）"""
    have = {int(b["timestamp"]) for b in entry.bars}
    front = [b for b in page if int(b["timestamp"]) not in have]
    entry.bars = sorted(front + entry.bars, key=lambda b: int(b["timestamp"]))


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
        touches_latest = end_time is None or (bool(e.bars) and end_time >= int(e.bars[-1]["timestamp"]))
        if touches_latest:
            tail_n = _TAIL_REFRESH if e.bars else limit
            try:
                tail = await _fetch_page(source, symbol, timeframe, tail_n, None)
            except Exception:
                if not e.bars:
                    raise   # 首拉失败无存量兜底，向上抛由路由兜底
                tail = []   # 有存量：刷新失败退回缓存（可能陈旧一根）
            if tail:
                if e.bars:
                    _merge_tail(e, tail)
                else:
                    e.bars = sorted(tail, key=lambda b: int(b["timestamp"]))
                # 不据短返回推定尽头：部分源裸拉有内部单次上限（如 thsdk ~6000），
                # 短返回可能是截断而非全量；尽头只由补拉路径的空页/短页判定

        # 缺口补拉：请求深度超过已缓存最旧根
        need = limit - _count_le(e.bars, end_time)
        anchor = (int(e.bars[0]["timestamp"]) - 1) if e.bars else end_time
        while need > 0 and not e.exhausted:
            take = min(max(need, 1000), 8000)
            try:
                page = await _fetch_page(source, symbol, timeframe, take, anchor)
            except Exception as ex:
                logger.warning(f"kline cache fill failed [{source.name}] {symbol}/{timeframe}: {ex}")
                break   # 保留已有数据退回；不标尽头，源恢复后下次请求可继续补拉
            if not page:
                e.exhausted = True
                break
            _merge_front(e, page)
            if len(page) < take:
                e.exhausted = True
                break
            anchor = int(page[0]["timestamp"]) - 1
            need = limit - _count_le(e.bars, end_time)

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


def clear() -> None:
    """清空缓存（测试/调试用）"""
    _cache.clear()
    _locks.clear()
