"""IG Markets REST 客户端

IG API 特性（与币安等公开行情不同）：
    - 所有接口（含行情）强制鉴权：X-IG-API-KEY 头 + 会话登录（CST + X-SECURITY-TOKEN）
    - 品种用 epic 标识（如 CSFRXNPZGBYUZ），需通过 /markets 搜索解析
    - /prices 单次 max 上限 1000，更早历史用 endDate 分页
    - 会话约 10 分钟无活动过期，请求前惰性续期

时间格式：v3 /prices 返回 snapshotTimeUTC（ISO '2026-08-07T16:33:00'，UTC），
回退解析 v1 风格 '2026:08:07-10:30:00'（可能带毫秒尾缀）
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

HTTP_PROXY = os.getenv("HTTP_PROXY", "http://127.0.0.1:7897")

# 前端周期 → IG /prices resolution
RESOLUTION_MAP = {
    "1m": "MINUTE", "3m": "MINUTE_3", "5m": "MINUTE_5", "15m": "MINUTE_15",
    "30m": "MINUTE_30", "1h": "HOUR", "2h": "HOUR_2", "4h": "HOUR_4", "1d": "DAY",
}

# 常用外汇/贵金属 epic 内置映射（demo 环境实测：spot CFD 命名 CS.D.{PAIR}.CFD.IP，
# 黄金为 MT.D.GC.*；缺失时自动走 /markets 搜索补全）
EPIC_MAP = {
    "EURUSD": "CS.D.EURUSD.CFD.IP",
    "GBPUSD": "CS.D.GBPUSD.CFD.IP",
    "USDJPY": "CS.D.USDJPY.CFD.IP",
    "AUDUSD": "CS.D.AUDUSD.CFD.IP",
    "USDCHF": "CS.D.USDCHF.CFD.IP",
    "USDCAD": "CS.D.USDCAD.CFD.IP",
    "NZDUSD": "CS.D.NZDUSD.CFD.IP",
    "EURGBP": "CS.D.EURGBP.CFD.IP",
    "XAUUSD": "MT.D.GC.FWS3.IP",
}

PRICES_MAX_PER_REQ = 1000          # IG /prices 单次上限
SESSION_REFRESH_AFTER = 540.0      # 秒：会话惰性续期阈值（IG 约 10 分钟过期）
MIN_REQ_GAP = 1.1                  # 秒：IG allowance 每 Key 仅 1 req/s，全局限速留余量
ALLOWANCE_BACKOFF = 60.0           # 秒：触发限流后的退避时长（账户级冻结约 1 分钟）

# ─── 点位报价归一化 ───
# 汇率型货币对惯例报价 < 10；实测 demo 环境 EURUSD 以点位报价（汇率×10000，如 11561），
# 价格超出合理区间时除以 10000 还原为汇率
RATE_PAIRS = {"EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "EURGBP", "USDCHF", "USDCAD"}
NORM_THRESHOLD = 10.0
NORM_DIVISOR = 10000.0


def normalize_rate(symbol: str, price: float) -> float:
    """点位报价归一：汇率型货币对价格超合理区间（>10）视为点位（×10000），除回汇率"""
    if symbol in RATE_PAIRS and price > NORM_THRESHOLD:
        return price / NORM_DIVISOR
    return price


def normalize_bar(symbol: str, bar: dict) -> dict:
    """bar OHLC 归一化（时间戳/成交量不变）"""
    return {
        **bar,
        "open": normalize_rate(symbol, bar["open"]),
        "high": normalize_rate(symbol, bar["high"]),
        "low": normalize_rate(symbol, bar["low"]),
        "close": normalize_rate(symbol, bar["close"]),
    }


class IgAuthError(RuntimeError):
    """IG 登录失败（凭证错误等）"""


def parse_ig_time(ts: str) -> int:
    """IG 时间 → Unix ms（UTC）。

    支持两种格式：
        - v3 snapshotTimeUTC：'2026-08-07T16:33:00'（ISO，UTC）
        - v1 snapshotTime：'2026:08:07-10:30:00'（或带 .SSS 毫秒）
    """
    s = ts.strip()
    if "T" in s:
        base, _, frac = s.partition(".")
        dt = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        ms = int((frac + "000")[:3]) if frac else 0
        return int(dt.timestamp() * 1000) + ms
    ms = 0
    if "." in s:
        s, _, frac = s.partition(".")
        ms = int((frac + "000")[:3])
    dt = datetime.strptime(s, "%Y:%m:%d-%H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000) + ms


class IgClient:
    """IG REST 客户端（会话管理 + 行情查询）"""

    def __init__(
        self,
        api_key: str | None = None,
        identifier: str | None = None,
        password: str | None = None,
        api_base: str | None = None,
    ):
        self.api_key = api_key or os.getenv("IG_API_KEY", "")
        self.identifier = identifier or os.getenv("IG_IDENTIFIER", "")
        self.password = password or os.getenv("IG_PASSWORD", "")
        self.api_base = (api_base or os.getenv("IG_API_BASE", "https://demo-api.ig.com/gateway/deal")).rstrip("/")
        self._cst: str | None = None
        self._token: str | None = None
        self._session_at: float = 0.0
        self._lightstreamer_endpoint: str | None = None
        self._epic_cache: dict[str, str] = dict(EPIC_MAP)
        self._login_lock = asyncio.Lock()
        self.account_id: str | None = None          # 登录响应 currentAccount.accountId
        # 全局限速：IG allowance 每 API Key 仅 1 req/s（多源实例共享同一 Key 也共享此闸）
        self._req_lock = asyncio.Lock()
        self._next_req_at: float = 0.0

    @property
    def available(self) -> bool:
        return bool(self.api_key and self.identifier and self.password)

    # ─── HTTP 基础设施 ───

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(proxy=HTTP_PROXY or None, timeout=15.0)

    def _headers(self, extra: dict | None = None, version: str = "2") -> dict[str, str]:
        h = {
            "X-IG-API-KEY": self.api_key,
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "application/json; charset=UTF-8",
            "Version": version,
        }
        if self._cst:
            h["CST"] = self._cst
        if self._token:
            h["X-SECURITY-TOKEN"] = self._token
        if extra:
            h.update(extra)
        return h

    async def _throttle(self) -> None:
        """全局限速：相邻请求间隔 ≥ MIN_REQ_GAP，防 exceeded-api-key-allowance"""
        async with self._req_lock:
            wait = self._next_req_at - time.monotonic()
            self._next_req_at = time.monotonic() + max(wait, 0.0) + MIN_REQ_GAP
        if wait > 0:
            await asyncio.sleep(wait)

    async def login(self) -> None:
        """POST /session（v2）建立会话，缓存 CST/SecurityToken/Lightstreamer 端点"""
        async with self._login_lock:
            if self._cst and (time.monotonic() - self._session_at) < SESSION_REFRESH_AFTER:
                return
            await self._throttle()   # 登录同样计入 allowance 配额
            async with self._client() as client:
                resp = await client.post(
                    f"{self.api_base}/session",
                    headers=self._headers(),
                    json={"identifier": self.identifier, "password": self.password},
                )
            if resp.status_code == 403 and "allowance" in resp.text:
                # 登录也计入配额：限流时退避后重试一次
                logger.warning(f"IG allowance exceeded on login, backoff {ALLOWANCE_BACKOFF:.0f}s")
                async with self._req_lock:
                    self._next_req_at = max(
                        self._next_req_at, time.monotonic() + ALLOWANCE_BACKOFF
                    )
                await asyncio.sleep(ALLOWANCE_BACKOFF)
                async with self._client() as client:
                    resp = await client.post(
                        f"{self.api_base}/session",
                        headers=self._headers(),
                        json={"identifier": self.identifier, "password": self.password},
                    )
            if resp.status_code != 200:
                raise IgAuthError(f"IG login failed: HTTP {resp.status_code} {resp.text[:200]}")
            self._cst = resp.headers.get("CST")
            self._token = resp.headers.get("X-SECURITY-TOKEN")
            if not self._cst:
                raise IgAuthError("IG login failed: no CST header in response")
            self._session_at = time.monotonic()
            try:
                body = resp.json()
                self._lightstreamer_endpoint = (
                    body.get("lightstreamerEndpoint") or self._lightstreamer_endpoint
                )
                # demo 登录响应无 currentAccount 对象：顶层 currentAccountId + accounts 数组
                acc_id = body.get("currentAccountId")
                if not acc_id:
                    accs = body.get("accounts") or []
                    acc_id = next(
                        (a.get("accountId") for a in accs
                         if a.get("preferred") or a.get("accountId")),
                        None,
                    )
                    if not acc_id:
                        acc = body.get("currentAccount") or {}
                        acc_id = acc.get("accountId")
                self.account_id = acc_id or self.account_id
                logger.info(
                    f"IG session established: account={self.account_id} "
                    f"ls_endpoint={self._lightstreamer_endpoint}"
                )
            except Exception:
                pass

    @property
    def lightstreamer_endpoint(self) -> str | None:
        return self._lightstreamer_endpoint

    @property
    def cst(self) -> str | None:
        return self._cst

    async def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        _retry: bool = True,
        version: str = "2",
    ) -> Any:
        """带会话管理的请求：限速 + 惰性续期 + 401 重登 + 403 限流退避（/prices 需 Version 3）"""
        await self.login()
        await self._throttle()
        async with self._client() as client:
            resp = await client.request(
                method,
                f"{self.api_base}{path}",
                headers=self._headers(version=version),
                params=params,
            )
        if resp.status_code == 401 and _retry:
            logger.info("IG session expired, re-login...")
            self._cst = None
            self._token = None
            return await self._request(method, path, params, _retry=False, version=version)
        if resp.status_code == 403 and "allowance" in resp.text and _retry:
            # 限流（Key/账户级）：退避后重试一次，并把后续请求的放行时间整体后移
            logger.warning(f"IG allowance exceeded on {path}, backoff {ALLOWANCE_BACKOFF:.0f}s")
            async with self._req_lock:
                self._next_req_at = max(
                    self._next_req_at, time.monotonic() + ALLOWANCE_BACKOFF
                )
            await asyncio.sleep(ALLOWANCE_BACKOFF)
            return await self._request(method, path, params, _retry=False, version=version)
        if resp.status_code != 200:
            raise RuntimeError(f"IG API {path} -> HTTP {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    # ─── epic 解析 ───

    async def resolve_epic(self, symbol: str) -> str:
        """品种名 → epic：内置映射优先，缺失则 /markets 搜索并缓存

        demo /markets 返回平铺结构（顶层 epic/instrumentType），优先选
        CURRENCIES/COMMODITIES 现货且 epic 含 '.CFD.' 的结果（排除期权/迷你合约）。
        """
        sym = symbol.upper()
        if sym in self._epic_cache:
            return self._epic_cache[sym]
        data = await self._request("GET", "/markets", params={"searchTerm": sym})
        candidates = []
        for m in data.get("markets") or []:
            inst = m.get("instrument") or m  # 兼容嵌套/平铺两种返回结构
            epic = inst.get("epic")
            itype = inst.get("instrumentType") or inst.get("type")
            if not epic or itype not in ("CURRENCIES", "COMMODITIES"):
                continue
            candidates.append(epic)
        if candidates:
            epic = next((e for e in candidates if ".CFD." in e), candidates[0])
            self._epic_cache[sym] = epic
            logger.info(f"IG epic resolved: {sym} -> {epic}")
            return epic
        raise RuntimeError(f"IG epic not found for symbol: {symbol}")

    # ─── 行情 ───

    async def fetch_prices(
        self,
        epic: str,
        resolution: str,
        limit: int,
        end_time: int | None = None,
    ) -> list[dict]:
        """历史 K 线：单次 max≤1000，超出用 endDate 向前分页拼接（升序返回）"""
        collected: list[dict] = []
        end_date: str | None = (
            datetime.fromtimestamp(end_time / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            if end_time else None
        )
        remaining = limit
        while remaining > 0:
            batch_n = min(remaining, PRICES_MAX_PER_REQ)
            params: dict[str, Any] = {"resolution": resolution, "max": batch_n}
            if end_date:
                params["endDate"] = end_date
            data = await self._request("GET", f"/prices/{epic}", params=params, version="3")
            prices = data.get("prices") or []
            if not prices:
                break
            collected.extend(prices)
            remaining -= len(prices)
            if len(prices) < batch_n:
                break  # 已到数据起点
            # 以本批最早一根的开始时间作为下一页终点（IG endDate 为不含边界）
            first_ts = _bar_ts(prices[0])
            end_date = datetime.fromtimestamp(
                (first_ts - 1000) / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%S")
        # 去重（分页边界可能重叠）+ 升序
        dedup: dict[int, dict] = {}
        for p in collected:
            dedup[_bar_ts(p)] = p
        return [dedup[k] for k in sorted(dedup)]

    @staticmethod
    def candle_to_bar(p: dict, event_ms: int = 0) -> dict:
        """IG candle → 标准 bar dict（取 bid 价；OTC 无成交量置 0）

        兼容两种结构：v3（openPrice/highPrice/lowPrice/closePrice 的 bid 字段）
        与 v1（lastBid 的 openBid/highBid/lowBid/closeBid）。
        """
        if "openPrice" in p:
            bid_of = lambda key: float(((p.get(key) or {}).get("bid")) or 0)
            return {
                "timestamp": _bar_ts(p),
                "open": bid_of("openPrice"),
                "high": bid_of("highPrice"),
                "low": bid_of("lowPrice"),
                "close": bid_of("closePrice"),
                "volume": 0.0,
                "event_ms": event_ms,
            }
        bid = p.get("lastBid") or {}
        return {
            "timestamp": _bar_ts(p),
            "open": float(bid.get("openBid", 0)),
            "high": float(bid.get("highBid", 0)),
            "low": float(bid.get("lowBid", 0)),
            "close": float(bid.get("closeBid", 0)),
            "volume": 0.0,
            "event_ms": event_ms,
        }

    async def fetch_latest_candle(self, epic: str, resolution: str) -> dict | None:
        """最新一根 K 线（降级轮询用）"""
        data = await self._request(
            "GET", f"/prices/{epic}", params={"resolution": resolution, "max": 1}, version="3"
        )
        prices = data.get("prices") or []
        return prices[-1] if prices else None


def _bar_ts(p: dict) -> int:
    """candle 开始时间：优先 v3 snapshotTimeUTC（ISO/UTC），回退 v1 snapshotTime"""
    return parse_ig_time(p.get("snapshotTimeUTC") or p["snapshotTime"])
