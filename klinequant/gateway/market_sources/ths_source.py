"""同花顺 A 股市场源插件（thsdk 协议 SDK）

接入方式：panghu11033/thsdk 封装同花顺原生协议直连行情服务器。历史深度
日 K 约 12 年、1m 约 8 个交易日（正式账户）；快照毫秒级，官方限频 20ms/次。

实时链路：market_data_cn 批量快照轮询（沪深必须分批）+ 本地 m1 聚合——
快照只有现价与当日累计量，5m~1h 由盘中 m1 桶按 A 股交易时段网格聚合，
1d 直接由快照当日累计字段合成，1w 惰性以周 K 为种子叠加当日增量。
日/周 K 复权方式固定前复权（adjust=forward）。

同步库单连接 + 限频：全部调用经驱动层全局锁串行并节流（同 MT5 模式）。
凭证走 .env（THS_USERNAME/THS_PASSWORD，MAC 自动生成）；缺失时 thsdk
落游客模式（仅限开发，注册时告警）。
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from gateway.market_sources.base import MarketSource
from gateway.market_sources.manager import market_manager

try:
    from thsdk import THS as _THS  # type: ignore
    _HAS_THS = True
except ImportError:  # pragma: no cover
    _THS = None
    _HAS_THS = False

logger = logging.getLogger(__name__)

_BJ = timezone(timedelta(hours=8))   # 不依赖 zoneinfo（A 股无夏令时，固定 +8）

POLL_INTERVAL = float(os.getenv("THS_POLL_INTERVAL", "2.0"))   # 快照轮询间隔（秒）
TICKER_CACHE_TTL = 2.0      # ticker 缓存（秒）：与轮询节奏相当，合并前端并发请求
RECONNECT_COOLDOWN = 30.0   # 重连冷却（秒）
_MIN_SPACING = 0.03         # 驱动层调用最小间隔（秒，官方限频 20ms + 冗余）
_MIN_PREC = 2               # A 股最小价格变动 0.01 元，精度下限 2 位

# 前端周期 → thsdk klines interval（thsdk 原生：1m/5m/15m/30m/60m/day/week）
INTERVAL_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "60m", "1d": "day", "1w": "week",
}
TF_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 86400, "1w": 604800}
# A 股连续竞价时段（秒）：上午 09:30~11:30，下午 13:00~15:00
_SESSIONS = ((9 * 3600 + 1800, 11 * 3600 + 1800), (13 * 3600, 15 * 3600))
# 合法 4 位市场前缀（指数代码段含字母，如沪市上证指数 USHI1A0001）
_VALID_PREFIXES = frozenset({
    "USHA", "USHB", "USHD", "USHI", "USHJ", "USHP", "USHT",
    "USZA", "USZB", "USZD", "USZI", "USZJ", "USZP", "USTM",
})


def _to_epoch_ms(dt) -> int:
    """thsdk 时间 → epoch 毫秒（分钟线带北京时区；日/周 K 为 naive 北京时间）"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_BJ)
    return int(dt.timestamp() * 1000)


def _normalize_code(symbol: str) -> str:
    """品种符号归一化为 10 位 THSCODE（4 位市场前缀 + 6 位代码）

    6 位纯数字按首位映射：6/5/9 → 沪市，0/3 → 深市（000001 类两义代码
    必须显式带前缀，如沪市上证指数 USHI1A0001 / 深市平安银行 USZA000001）。
    """
    s = symbol.strip().upper()
    if len(s) == 10 and s[:4] in _VALID_PREFIXES:
        return s
    if len(s) == 6 and s.isdigit():
        if s[0] in "569":
            return "USHA" + s
        if s[0] in "03":
            return "USZA" + s
    raise ValueError(f"invalid A-share symbol: {symbol}")


#: 沪指数无算术规则段 → 交易所码静态表（实测 2026-08-31）：
#: 1A0001=上证指数；1B0001~1B0005=行业分类指数（工业/商业/地产/公用，
#: 交易所码 000004~000008，若不映射会与上证指数撞码 000001）
_SH_INDEX_FIXED = {
    "USHI1A0001": "000001",   # 上证指数
    "USHI1C0003": "000001",   # 上证指数（目录同名重复条目，展示码同 1A0001）
    "USHI1A0002": "000002",   # 上证Ａ股指数（实测）
    "USHI1A0003": "000003",   # 上证Ｂ股指数（实测）
    "USHI1B0001": "000004",   # 工业指数
    "USHI1B0002": "000005",   # 商业指数
    "USHI1B0004": "000007",   # 地产指数（1B0003 不在目录）
    "USHI1B0005": "000008",   # 公用指数
    # 领先指标(1C0002/3C0002)、创业成交(3C0003) 为内部编制指数，无交易所码，
    # 兜底保留原码段（不撞码不猜映射）
}


def _display_code(code: str) -> str:
    """THSCODE → 面向用户的展示码（纯数字交易所码，供前端直接渲染）

    股票/深指数取末 6 位；沪指数 1B 段取「00」+ 末 4 位（实测：
    1B0016=上证50→000016、1B0688=科创50→000688、1B0300=沪深300沪→000300）；
    无算术规则的（1A/1C 段、行业分类指数）走静态表；
    未命中时兜底末 6 位（可能含字母，仅展示不影响路由）。
    """
    if code[:4] == "USHI":
        if code in _SH_INDEX_FIXED:
            return _SH_INDEX_FIXED[code]
        if code[4:6] == "1B":
            return "00" + code[6:]
    return code[-6:]


def _m1_label(sec: int) -> int:
    """快照时刻（北京当日秒）→ m1 桶标签（收盘时刻惯例，同 thsdk 历史 1m 标签）

    桶覆盖 (t-1min, t]：09:30 开盘（含集合竞价）归首桶 09:31；11:30:00/15:00:00
    边界快照钳制回本时段末桶；午间/盘外快照归入最近的前一时段末桶。
    """
    if sec <= _SESSIONS[0][0] + 60:
        return _SESSIONS[0][0] + 60                    # 开盘及盘前 → 09:31
    label = (sec // 60 + 1) * 60
    for start, end in _SESSIONS:
        if start < label <= end + 60:
            return min(label, end)
    # 午休归上午尾桶，盘后归收盘桶（快照静态无成交，仅防桶位漂移）
    return _SESSIONS[0][1] if sec < _SESSIONS[1][0] else _SESSIONS[-1][1]


def _bucket_label(label_sec: int, tf_sec: int) -> int:
    """m1 桶标签 → tf 桶标签（时段内按开盘时刻对齐网格，跨时段不串桶）"""
    for start, end in _SESSIONS:
        if start < label_sec <= end:
            n = (label_sec - start + tf_sec - 1) // tf_sec
            return start + n * tf_sec
    return label_sec


class ThsApi:
    """真实驱动：包装 thsdk 同步调用（全局锁串行 + 限频节流 + 断线重连）

    返回值约定：失败/未连接返回 None；成功但无数据返回 []。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ths = None
        self.connected = False
        self.is_guest = False
        self._last_call = 0.0
        self._last_connect_at = 0.0

    # ─── 连接管理（锁内调用） ───

    def _ensure_locked(self) -> bool:
        if self.connected and self._ths is not None:
            return True
        if not _HAS_THS:
            return False
        now = time.monotonic()
        if now - self._last_connect_at < RECONNECT_COOLDOWN:
            return False
        self._last_connect_at = now
        try:
            ths = _THS()   # 凭证自动读 THS_USERNAME/THS_PASSWORD 环境变量
            r = ths.connect()
            if r.success:
                self._ths = ths
                self.connected = True
                self.is_guest = str(ths.ops.get("username", "")).startswith("thsguest_")
                return True
            logger.warning(f"THS connect failed: {r.error}")
        except Exception as e:  # pragma: no cover
            logger.warning(f"THS connect error: {e}")
        return False

    def _mark_broken(self) -> None:
        self.connected = False
        try:
            if self._ths is not None:
                self._ths.disconnect()
        except Exception:
            pass
        self._ths = None

    # ─── 统一调用入口 ───

    def _call(self, fn, retries: int = 2):
        """锁内：限频节流 → 执行 → 限频错误重试 → 断连标记"""
        with self._lock:
            if not self._ensure_locked():
                return None
            for attempt in range(retries + 1):
                wait = _MIN_SPACING - (time.monotonic() - self._last_call)
                if wait > 0:
                    time.sleep(wait)
                self._last_call = time.monotonic()
                try:
                    r = fn(self._ths)
                except Exception as e:
                    logger.debug(f"THS call exception: {e}")
                    self._mark_broken()
                    return None
                if r.success:
                    return r.data if r.data is not None else []
                err = r.error or ""
                if "太快" in err or "限频" in err:
                    if attempt < retries:
                        time.sleep(0.05)
                        continue
                if "未登录" in err or "未连接" in err:
                    self._mark_broken()
                logger.debug(f"THS call failed: {err}")
                return None
            return None  # pragma: no cover

    def connect(self) -> bool:
        with self._lock:
            return self._ensure_locked()

    # ─── 数据接口 ───

    def klines(self, code: str, interval: str, count: int | None = None,
               start: datetime | None = None, end: datetime | None = None):
        if count is not None:
            return self._call(
                lambda t: t.klines(code, interval=interval, count=count, adjust="forward")
            )
        return self._call(
            lambda t: t.klines(
                code, interval=interval, start_time=start, end_time=end, adjust="forward"
            )
        )

    def snapshot(self, codes: list[str]):
        """批量快照（调用方保证同市场前缀，跨所分批）"""
        return self._call(lambda t: t.market_data_cn(codes))

    def stock_list(self):
        return self._call(lambda t: t.stock_cn_lists())

    def index_list(self):
        return self._call(lambda t: t.index_list())


class _SymState:
    """单品种盘中聚合状态（m1 桶 + 高周期累计 + 周 K 种子）"""

    __slots__ = ("date", "m1", "last_cum", "week")

    def __init__(self) -> None:
        self.date = None            # 状态所属交易日（北京 date）
        self.m1: dict[int, dict] = {}   # m1 桶标签(ms) -> bar
        self.last_cum = 0           # 最近一次快照的当日累计成交量
        self.week: dict | None = None   # 周 K 种子 {ts, open, high, low, volume, base, wk}


class ThsSource(MarketSource):
    """同花顺 A 股：历史 K 线（前复权）+ 快照轮询实时流"""

    name = "ths"
    label = "同花顺 A股"
    region = "cn"
    supported_timeframes = set(INTERVAL_MAP.keys())
    supports_volume = True
    # 沪指数代码不可直拼（上证指数=USHI1A0001，经 index_list 名称映射实测确认）
    default_symbols = [
        {"symbol": "USHI1A0001", "name": "上证指数", "type": "index", "code": "000001"},
        {"symbol": "USZI399001", "name": "深证成指", "type": "index", "code": "399001"},
        {"symbol": "USZI399300", "name": "沪深300", "type": "index", "code": "399300"},
        {"symbol": "USHA600519", "name": "贵州茅台", "type": "stock", "code": "600519"},
        {"symbol": "USZA300750", "name": "宁德时代", "type": "stock", "code": "300750"},
        {"symbol": "USHA601318", "name": "中国平安", "type": "stock", "code": "601318"},
        {"symbol": "USZA002594", "name": "比亚迪", "type": "stock", "code": "002594"},
    ]
    watched_targets: list[tuple[str, str]] = []

    def __init__(self, driver: ThsApi | None = None):
        self._driver = driver or ThsApi()
        self.available = self._driver.connect()
        if self.available and getattr(self._driver, "is_guest", False):
            logger.warning(
                "THS source running in GUEST mode"
                "（游客模式仅限开发，请配置 THS_USERNAME/THS_PASSWORD）"
            )
        self._states: dict[str, _SymState] = {}
        self._sym_tfs: dict[str, set[str]] = {}     # code -> 当前订阅周期集
        self._ticker_cache: dict[str, tuple[float, dict | None]] = {}

    # ─── 精度：A 股固定 2 位下限，订阅数据推导抬升 ───

    def price_precision(self, symbol: str) -> int:
        return max(super().price_precision(symbol), _MIN_PREC)

    # ─── 全量品种目录（股票 + 指数） ───

    async def list_symbols(self) -> list[dict]:
        stocks = await asyncio.to_thread(self._driver.stock_list)
        indexes = await asyncio.to_thread(self._driver.index_list)
        if stocks is None and indexes is None:
            return await super().list_symbols()
        out = []
        for rows, atype in ((stocks, "stock"), (indexes, "index")):
            for row in rows or []:
                code, name = row.get("代码"), row.get("名称")
                if code and name:
                    out.append({
                        "symbol": code, "name": name, "type": atype,
                        "code": _display_code(code),
                    })
        return out or await super().list_symbols()

    # ─── REST 历史 K 线 / 行情摘要 ───

    def _to_bar(self, row: dict) -> dict:
        return {
            "timestamp": _to_epoch_ms(row["时间"]),
            "open": float(row["开盘价"]),
            "high": float(row["最高价"]),
            "low": float(row["最低价"]),
            "close": float(row["收盘价"]),
            "volume": float(row["成交量"]),   # 单位：手
            "event_ms": int(time.time() * 1000),
        }

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
        end_time: int | None = None,
    ) -> list[dict]:
        interval = INTERVAL_MAP.get(timeframe)
        if not interval:
            raise ValueError(f"THS unsupported timeframe: {timeframe}")
        code = _normalize_code(symbol)
        if end_time:
            # 翻页加深：按 end_time（含）向前回溯（窗口留 2 倍冗余防缺口）
            dt_to = datetime.fromtimestamp(end_time / 1000, tz=timezone.utc)
            dt_from = dt_to - timedelta(seconds=TF_SECONDS[timeframe] * limit * 2)
            rows = await asyncio.to_thread(
                self._driver.klines, code, interval, None, dt_from, dt_to
            )
        else:
            rows = await asyncio.to_thread(self._driver.klines, code, interval, limit)
        if rows is None:
            raise RuntimeError(f"THS klines failed for {code}/{timeframe}")
        bars = [self._to_bar(r) for r in rows]
        for b in bars[-20:]:
            self._track_prec(symbol, [b["open"], b["high"], b["low"], b["close"]])
        if end_time:
            bars = [b for b in bars if b["timestamp"] <= end_time]
        return bars[-limit:]

    async def fetch_ticker(self, symbol: str) -> dict | None:
        cached = self._ticker_cache.get(symbol)
        if cached and time.monotonic() - cached[0] < TICKER_CACHE_TTL:
            return cached[1]
        code = _normalize_code(symbol)
        rows = await asyncio.to_thread(self._driver.snapshot, [code])
        if not rows:
            return cached[1] if cached else None
        row = rows[0]
        last = float(row.get("价格") or 0)
        if last <= 0:   # 停牌/无报价
            return cached[1] if cached else None
        prev = float(row.get("昨收价") or 0)
        self._track_prec(symbol, [last])
        result = {
            "symbol": code,
            "last_price": last,
            "bid": last,   # 快照无盘口，最新价兜底
            "ask": last,
            "volume_24h": float(row.get("成交量") or 0),
            "price_change_pct": (last / prev - 1) * 100 if prev > 0 else 0.0,
            "high_24h": float(row.get("最高价") or last),
            "low_24h": float(row.get("最低价") or last),
        }
        self._ticker_cache[symbol] = (time.monotonic(), result)
        return result

    # ─── 实时流：快照轮询 + 本地聚合 ───

    async def stream_loop(self) -> None:
        if not self.available:
            logger.warning("THS source unavailable: connect failed, stream loop idle")
            while True:
                await asyncio.sleep(60)
        logger.info(f"THS source stream started (mode=snapshot-poll, interval={POLL_INTERVAL}s)")
        while True:
            now_bj = datetime.now(_BJ)
            if not self._session_open(now_bj):
                await asyncio.sleep(30)
                continue
            targets = market_manager.active_targets(self.name)
            if not targets:
                await asyncio.sleep(2)
                continue
            tfs_by_code: dict[str, set[str]] = {}
            for symbol, tf in targets:
                try:
                    code = _normalize_code(symbol)
                except ValueError:
                    continue
                tfs_by_code.setdefault(code, set()).add(tf)
            if not tfs_by_code:
                await asyncio.sleep(2)
                continue
            self._sym_tfs = tfs_by_code
            today = now_bj.date()
            # 批量快照：同一市场前缀一批（沪深/指数分批）
            groups: dict[str, list[str]] = {}
            for code in tfs_by_code:
                groups.setdefault(code[:4], []).append(code)
            snaps: dict[str, dict] = {}
            for codes in groups.values():
                rows = await asyncio.to_thread(self._driver.snapshot, codes)
                if rows is None:
                    self._try_reconnect()
                    break
                for row in rows:
                    if row.get("代码"):
                        snaps[row["代码"]] = row
            for code, row in snaps.items():
                try:
                    await self._update_symbol(code, row, today, now_bj)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.debug(f"THS update error {code}: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    @staticmethod
    def _session_open(dt: datetime) -> bool:
        """A 股轮询窗口：工作日 09:15~15:05（含集合竞价观察期）"""
        if dt.weekday() >= 5:
            return False
        t = dt.hour * 3600 + dt.minute * 60 + dt.second
        return 9 * 3600 + 900 <= t <= 15 * 3600 + 300

    async def _update_symbol(self, code: str, row: dict, today, now_bj: datetime) -> None:
        st = self._states.get(code)
        if st is None or st.date != today:
            st = _SymState()
            st.date = today
            self._states[code] = st
            await self._seed_today(code, st)
        price = float(row.get("价格") or 0)
        if price <= 0:   # 停牌/未开盘无报价
            return
        cum = int(row.get("成交量") or 0)
        self._track_prec(code, [price])
        sec = now_bj.hour * 3600 + now_bj.minute * 60 + now_bj.second
        base_ms = int(datetime.combine(today, datetime.min.time(), tzinfo=_BJ).timestamp() * 1000)
        label_ms = base_ms + _m1_label(sec) * 1000
        # ── m1 桶：OHLC 推进 + 累计量差分（首帧快照只建基线不计差分，防与种子重复）──
        delta = max(cum - st.last_cum, 0) if st.last_cum else 0
        st.last_cum = cum
        bar = st.m1.get(label_ms)
        if bar is None:
            st.m1[label_ms] = bar = {
                "timestamp": label_ms, "open": price, "high": price, "low": price,
                "close": price, "volume": 0.0,
            }
            if len(st.m1) > 300:   # 当日桶上限，防御性淘汰最旧
                del st.m1[min(st.m1)]
        bar["high"] = max(bar["high"], price)
        bar["low"] = min(bar["low"], price)
        bar["close"] = price
        bar["volume"] += delta
        tfs = self._sym_tfs[code]
        if "1m" in tfs:
            await self._publish(code, "1m", bar)
        # ── 5m~1h：当日 m1 桶按交易时段网格聚合 ──
        label_sec = label_ms // 1000 - base_ms // 1000
        for tf in ("5m", "15m", "30m", "1h"):
            if tf not in tfs:
                continue
            bucket_sec = _bucket_label(label_sec, TF_SECONDS[tf])
            members = [
                b for ts, b in st.m1.items()
                if _bucket_label(ts // 1000 - base_ms // 1000, TF_SECONDS[tf]) == bucket_sec
            ]
            if not members:
                continue
            members.sort(key=lambda b: b["timestamp"])
            await self._publish(code, tf, {
                "timestamp": base_ms + bucket_sec * 1000,
                "open": members[0]["open"],
                "high": max(b["high"] for b in members),
                "low": min(b["low"] for b in members),
                "close": members[-1]["close"],
                "volume": sum(b["volume"] for b in members),
            })
        # ── 1d：快照当日累计字段直接合成 ──
        if "1d" in self._sym_tfs[code]:
            await self._publish(code, "1d", {
                "timestamp": base_ms,
                "open": float(row.get("开盘价") or price),
                "high": float(row.get("最高价") or price),
                "low": float(row.get("最低价") or price),
                "close": price,
                "volume": float(cum),
            })
        # ── 1w：周 K 种子 + 当日增量 ──
        if "1w" in self._sym_tfs[code]:
            wk = await self._week_bar(code, st, row, price, cum)
            if wk:
                await self._publish(code, "1w", wk)

    async def _publish(self, code: str, tf: str, bar: dict) -> None:
        bar = dict(bar)
        bar["event_ms"] = int(time.time() * 1000)
        await market_manager.publish_bar(self.name, code, tf, bar)

    async def _seed_today(self, code: str, st: _SymState) -> None:
        """流启动/跨日首帧：拉当日已有 1m 历史填充聚合状态（失败则冷启动）"""
        if not st.date:
            return
        rows = await asyncio.to_thread(self._driver.klines, code, "1m", 250)
        if not rows:
            return
        day_ms = int(datetime.combine(st.date, datetime.min.time(), tzinfo=_BJ).timestamp() * 1000)
        for r in rows:
            bar = self._to_bar(r)
            if bar["timestamp"] >= day_ms:
                st.m1[bar["timestamp"]] = bar
        # last_cum 保持 0：首帧快照的累计量只建基线，种子已含此前成交

    async def _week_bar(
        self, code: str, st: _SymState, row: dict, price: float, cum: int,
    ) -> dict | None:
        wk = st.week
        today = st.date
        wk_key = today.isocalendar()[:2] if today else None
        if wk is None or wk.get("wk") != wk_key:
            # 种子：最近一根周 K + 当日日 K（其成交量为种子时刻的当日累计，作增量基线）
            wrows = await asyncio.to_thread(self._driver.klines, code, "week", 2)
            drows = await asyncio.to_thread(self._driver.klines, code, "day", 1)
            if not wrows:
                return None
            seed = wrows[-1]
            seed_ts = _to_epoch_ms(seed["时间"])
            seed_wk = datetime.fromtimestamp(seed_ts / 1000, _BJ).isocalendar()[:2]
            if seed_wk == wk_key:
                wk = st.week = {
                    "ts": seed_ts,
                    "open": float(seed["开盘价"]),
                    "high": float(seed["最高价"]),
                    "low": float(seed["最低价"]),
                    "volume": float(seed["成交量"]),
                    "base": float(drows[-1]["成交量"]) if drows else 0.0,
                    "wk": wk_key,
                }
            else:
                # 种子落在上一周（周一开盘瞬间等边界）：本周仅用当日数据起步，
                # 周开盘价等随后续日 K 种子自然补齐（下一轮 wk_key 不变不重种）
                day_ts = int(
                    datetime.combine(today, datetime.min.time(), tzinfo=_BJ).timestamp() * 1000
                )
                wk = st.week = {
                    "ts": day_ts,
                    "open": float(row.get("开盘价") or price),
                    "high": float(row.get("最高价") or price),
                    "low": float(row.get("最低价") or price),
                    "volume": float(cum),
                    "base": float(cum),
                    "wk": wk_key,
                }
        delta = max(cum - wk["base"], 0)
        return {
            "timestamp": wk["ts"],
            "open": wk["open"],
            "high": max(wk["high"], float(row.get("最高价") or price)),
            "low": min(wk["low"], float(row.get("最低价") or price)),
            "close": price,
            "volume": wk["volume"] + delta,
        }

    def _try_reconnect(self) -> None:
        """快照批次失败时触发驱动层重连（冷却在驱动侧节流）"""
        if not self._driver.connect():
            return
        self.available = True
        logger.info("THS source reconnected")
