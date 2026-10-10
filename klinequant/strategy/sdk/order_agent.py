"""Phase 3 follower 侧 OrderAgent — 白名单指令跟单执行器（不含任何策略代码）。

信任边界（follower 端，一等约束）：
  - 只订阅 lead 的 ``intent`` / ``heartbeat`` 通道，只接受白名单 ``msg_type``
    （DIST_INTENT / DIST_CANCEL / DIST_FLATTEN / DIST_HEARTBEAT）且 token 匹配的消息；
    非白名单 / token 不符 → **直接丢弃 + 告警，绝不执行**，更不接受任何策略代码或参数。
  - 执行完全在 follower 本地：``intent → 幂等去重 → 缩放 → 品种映射 → 本地
    UnifiedResolver/Executor（经 KqApi）下单 → 回报回流``。

幂等：主键 = lead 侧 WAL 生成的 ``client_order_id``（全局唯一、重启续号），落
``AgentDedup``（SQLite），重放/重启后仍只执行一次。

心跳：记录最近 heartbeat 时刻；龄 > ``lead_timeout`` → ``lead_lost=True`` → 此后 OPEN
intent 一律 **hold**（回报 REJECT reason="lead_lost: hold"），CLOSE/flatten 放行（只减
风险）。``on_lead_lost=hold`` 固定；自动 flatten / 状态对账留给 ACC-SYNC 专项。

线程/循环模型：本类 async-native，主线程跑独立 asyncio 循环（Windows 必须
SelectorEventLoop）；ZMQ 收发在循环上，本地同步执行（MT5 / KqApi）经
``asyncio.to_thread`` offload，避免阻塞 ZMQ 循环。
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
import sys
import threading
import time
from collections.abc import Callable
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any

from core.trade_engine.ledger import ExposureLedger
from core.trade_engine.resolver import UnifiedResolver
from protocol.transport.zmq_transport import ZmqTransport
from protocol.types import Offset, OrderKind, OrderSide, Tif
from strategy.sdk.api import KqApi
from strategy.sdk.dist_protocol import (
    ACTION_CANCEL,
    ACTION_FLATTEN,
    DEFAULT_INTENT_ENDPOINT,
    DEFAULT_REPORT_ENDPOINT,
    TOPIC_HEARTBEAT,
    TOPIC_INTENT,
    TOPIC_REPORT,
    OrderIntent,
    Report,
    parse_endpoint,
    parse_message,
    report_to_message,
)
from strategy.sdk.order_id import OrderIdFactory

if TYPE_CHECKING:  # 仅类型标注，避免运行期耦合
    from core.notification.alert_manager import AlertManager
    from strategy.sdk.backend import MarketBackend

logger = logging.getLogger(__name__)

__all__ = ["OrderAgent", "AgentDedup"]


# ─────────────────────────────────────────────
# 幂等去重（SQLite 落盘，主键 = lead client_order_id）
# ─────────────────────────────────────────────
class AgentDedup:
    """follower 幂等去重表（重启后仍生效）。

    落盘范式对齐 ``order_journal.SqliteJournal``：WAL + synchronous=FULL + 一把
    ``threading.Lock`` + ``check_same_thread=False``（asyncio.to_thread 跨线程访问）。
    """

    def __init__(self, db_path: str | Path):
        self._path = str(db_path)
        if self._path != ":memory:":
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS dedup ("
            "coid TEXT PRIMARY KEY, status TEXT, ts INTEGER)"
        )
        self._conn.commit()

    def seen(self, coid: str) -> bool:
        """该 lead coid 是否已处理过。"""
        with self._lock:
            cur = self._conn.execute("SELECT 1 FROM dedup WHERE coid=?", (coid,))
            return cur.fetchone() is not None

    def mark(self, coid: str, status: str = "") -> None:
        """标记 coid 已处理（幂等主键落盘）。"""
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO dedup (coid, status, ts) VALUES (?,?,?)",
                (coid, status, int(time.time() * 1000)),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:  # pragma: no cover - 关停异常不阻断
                pass


# ─────────────────────────────────────────────
# 最小 feed 桩（KqApi 构造需要；agent 不驱动行情，stale_threshold=None 不触发心跳查询）
# ─────────────────────────────────────────────
class _NullFeed:
    """KqApi 依赖注入用的空数据源（OrderAgent 不消费行情，仅借 KqApi 执行通道）。"""

    def latest_tick(self, symbol: str) -> Any | None:
        return None

    def latest_bars(self, symbol: str, period: str, count: int) -> list:
        return []

    def wait_update(self, deadline: float | None = None) -> bool:
        return False

    def wait_and_snapshot(self, deadline=None, symbol=None):
        return False, {}

    def resolve_key(self, obj: Any, field: str | None = None) -> str | None:
        return None

    def current_version(self, key: str) -> int:
        return 0

    def is_changing(self, obj: Any, field: str | None = None) -> bool:
        return False

    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def seconds_since_update(self, symbol: str | None = None) -> float:
        return 0.0


# ─────────────────────────────────────────────
# OrderAgent
# ─────────────────────────────────────────────
class OrderAgent:
    """follower 侧跟单执行器（订阅 lead 白名单指令 → 本地执行 → 回报回流）。

    Args:
        token: 分发通道鉴权 token（必须与 lead 一致；明文只在 .env）。
        follower_account: follower 账户名（magic 派生 + 回报 follower 字段 + dedup 库名）。
        backend: 市场后端（Mt5Backend / BinanceBackend）；提供 connect/load_specs/
            make_executor。测试可注入 ``api=`` + ``specs=`` 绕过 backend。
        scale: fixed 缩放倍率（scale_mode="fixed" 时 qty×scale）。
        scale_mode: "fixed"（按 scale）| "balance"（按 follower/lead 余额比，缺失回落 fixed）。
        symbol_map: lead 品种 → follower 品种映射；空/None = 恒等映射（同名）；
            非空但缺项 → 跳过该 intent + 告警（绝不猜品种）。
        lead_intent_endpoint: lead intent/heartbeat 的 PUB 端点（agent SUB **connect**）。
        report_endpoint: lead report 的 SUB 端点（agent report PUB **connect**，反向扇入）。
        alert_manager: 可选告警管理器；None 时仅日志。
        lead_timeout: 心跳超时秒；龄超此值 → lead_lost → OPEN hold。
        symbols: follower 可交易品种（缺省取 symbol_map 的值域）。
        period: KqApi 驱动周期（agent 不驱动行情，仅记账 tag/magic 口径）。
        tag: follower 记账 tag（默认 = period）；magic 按 (follower_account, tag) 派生。
        dedup_path: 幂等库路径（默认 data/agent_dedup/{follower}.db）。
        api: 预构造 KqApi（测试注入；非 None 时跳过 backend 构建执行栈）。
        specs: 预构造品种规格（测试注入）。
        balance_provider: 可选 ``Callable[[], Decimal]`` 返回 follower 余额（balance 缩放用）。
        source: Message.source 标识（默认 "follower"）。
    """

    def __init__(
        self,
        *,
        token: str,
        follower_account: str,
        backend: MarketBackend | None = None,
        scale: Decimal | float | str = Decimal("1"),
        scale_mode: str = "fixed",
        symbol_map: dict[str, str] | None = None,
        lead_intent_endpoint: str = DEFAULT_INTENT_ENDPOINT,
        report_endpoint: str = DEFAULT_REPORT_ENDPOINT,
        alert_manager: AlertManager | None = None,
        lead_timeout: float = 10.0,
        symbols: list[str] | None = None,
        period: str = "1m",
        tag: str = "",
        dedup_path: str | Path | None = None,
        api: KqApi | None = None,
        specs: dict[str, Any] | None = None,
        balance_provider: Callable[[], Decimal] | None = None,
        source: str = "follower",
    ):
        self._token = token
        self._follower = follower_account
        self._backend = backend
        self._scale = Decimal(str(scale))
        self._scale_mode = scale_mode
        self._symbol_map: dict[str, str] = dict(symbol_map or {})
        self._intent_host, self._intent_port = parse_endpoint(lead_intent_endpoint)
        self._report_host, self._report_port = parse_endpoint(report_endpoint)
        self._alert = alert_manager
        self._lead_timeout = lead_timeout
        self._symbols = list(symbols) if symbols else []
        self._period = period
        self._tag = tag or period
        self._balance_provider = balance_provider
        self._source = source

        dedup_path = dedup_path or (
            Path("data") / "agent_dedup" / f"{follower_account}.db"
        )
        self._dedup = AgentDedup(dedup_path)

        # 执行栈（backend 构建 or 测试注入）
        self._api = api
        self._specs: dict[str, Any] = dict(specs or {})
        self._owns_stack = api is None

        # ZMQ / 循环状态
        self._sub: ZmqTransport | None = None
        self._pub: ZmqTransport | None = None
        self._hb_task: asyncio.Task | None = None
        self._last_hb = time.monotonic()
        self._lead_lost = False
        self._started = False

    # ═══════════ 生命周期 ═══════════

    def run(self) -> None:
        """阻塞运行（主线程独立 asyncio 循环；Ctrl+C 优雅退出）。"""
        if sys.platform == "win32":
            # Windows ZMQ asyncio 必须 SelectorEventLoop（Proactor 不支持 add_reader）
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        try:
            asyncio.run(self._run_main())
        except KeyboardInterrupt:
            logger.info("[AGENT] interrupted, stopped")

    async def _run_main(self) -> None:
        await self.start_async()
        logger.info("[AGENT] running... Ctrl+C to stop")
        try:
            while True:
                await asyncio.sleep(0.5)
        finally:
            await self.stop_async()

    async def start_async(self) -> None:
        """构建执行栈 + 绑定 ZMQ（SUB intent/heartbeat、PUB report）+ 起心跳监控。"""
        self._build_stack()
        # intent/heartbeat：agent SUB connect 到 lead PUB
        self._sub = ZmqTransport(
            role="subscriber", bind_host=self._intent_host,
            sub_port=self._intent_port, sub_bind=False,
        )
        await self._sub.start()
        await self._sub.subscribe(TOPIC_INTENT, self._on_intent_msg)
        await self._sub.subscribe(TOPIC_HEARTBEAT, self._on_heartbeat_msg)
        # report：agent PUB connect 到 lead SUB（反向扇入）
        self._pub = ZmqTransport(
            role="publisher", bind_host=self._report_host,
            pub_port=self._report_port, pub_bind=False,
        )
        await self._pub.start()
        self._last_hb = time.monotonic()
        self._hb_task = asyncio.get_event_loop().create_task(self._heartbeat_monitor())
        self._started = True
        logger.info(
            f"[AGENT] started (follower={self._follower} scale={self._scale} "
            f"mode={self._scale_mode} intent=tcp://{self._intent_host}:{self._intent_port} "
            f"report=tcp://{self._report_host}:{self._report_port})"
        )

    def _build_stack(self) -> None:
        """从 backend 构建本地执行栈（KqApi）；测试注入 api 时跳过。"""
        if self._api is not None:
            return
        if self._backend is None:
            raise ValueError("OrderAgent requires either backend or an injected api")
        self._backend.connect()
        syms = list(self._symbols)
        if not syms and self._symbol_map:
            syms = list(dict.fromkeys(self._symbol_map.values()))
        if not syms:
            raise ValueError("OrderAgent needs --symbols or a non-empty symbol_map")
        self._symbols = syms
        self._specs = self._backend.load_specs(syms)
        executor = self._backend.make_executor()
        ledger = ExposureLedger()
        resolver = UnifiedResolver(order_id_gen=OrderIdFactory().make)
        self._api = KqApi(
            symbol=syms[0], period=self._period, tag=self._tag,
            specs=self._specs, ledger=ledger, resolver=resolver, executor=executor,
            feed=_NullFeed(), account_name=self._follower,
            exchange=getattr(self._backend, "exchange", "mt5"),
        )

    async def stop_async(self) -> None:
        """停心跳监控 + 关 ZMQ + 关 dedup（best-effort）。"""
        self._started = False
        if self._hb_task is not None and not self._hb_task.done():
            self._hb_task.cancel()
            try:
                await self._hb_task
            except asyncio.CancelledError:
                pass
        if self._sub is not None:
            try:
                await self._sub.stop()
            except Exception as e:  # pragma: no cover
                logger.warning(f"[AGENT] sub stop error: {e}")
        if self._pub is not None:
            try:
                await self._pub.stop()
            except Exception as e:  # pragma: no cover
                logger.warning(f"[AGENT] pub stop error: {e}")
        self._dedup.close()
        logger.info("[AGENT] stopped")

    # ═══════════ ZMQ 回调（async，跑在 agent 循环上）═══════════

    async def _on_heartbeat_msg(self, msg) -> None:
        obj, reason = parse_message(msg, self._token)
        if obj is None:
            logger.warning(f"[AGENT] heartbeat rejected: {reason}")
            return
        self._last_hb = time.monotonic()
        if self._lead_lost:
            self._lead_lost = False
            logger.info("[AGENT] lead heartbeat recovered → resume OPEN")

    async def _on_intent_msg(self, msg) -> None:
        obj, reason = parse_message(msg, self._token)
        if obj is None:
            # 信任边界：非白名单 / token 不符 → 丢弃 + 告警，绝不执行
            logger.warning(f"[AGENT] intent rejected: {reason}")
            await self._alert_warn(f"rejected non-whitelist/token-mismatch intent: {reason}")
            return
        intent: OrderIntent = obj
        # 本地同步执行 offload 到线程，避免阻塞 ZMQ 循环
        report = await asyncio.to_thread(self._execute_intent, intent)
        await self._send_report(report)

    async def _heartbeat_monitor(self) -> None:
        interval = max(0.2, min(1.0, self._lead_timeout / 4))
        try:
            while True:
                age = time.monotonic() - self._last_hb
                lost = age > self._lead_timeout
                if lost and not self._lead_lost:
                    self._lead_lost = True
                    logger.warning(
                        f"[AGENT] lead lost (no heartbeat for {age:.1f}s "
                        f"> {self._lead_timeout:.0f}s) → hold OPEN"
                    )
                    await self._alert_warn(
                        f"lead heartbeat lost for {age:.1f}s → holding new OPEN orders"
                    )
                elif not lost and self._lead_lost:
                    self._lead_lost = False
                    logger.info("[AGENT] lead heartbeat recovered → resume OPEN")
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass

    async def _send_report(self, report: Report) -> None:
        if self._pub is None:
            return
        report.follower = report.follower or self._follower
        report.ts = report.ts or int(time.time() * 1000)
        try:
            await self._pub.publish(
                TOPIC_REPORT, report_to_message(report, self._token, source=self._source)
            )
        except Exception as e:  # 回报失败仅告警（best-effort）
            logger.warning(f"[AGENT] report publish failed: {e}")

    async def _alert_warn(self, message: str) -> None:
        if self._alert is None:
            return
        try:
            await self._alert.fire_warning(message, source="order_agent")
        except Exception as e:  # pragma: no cover - 告警失败不阻断
            logger.warning(f"[AGENT] alert fire error: {e}")

    # ═══════════ 核心执行（同步；由 to_thread 调用）═══════════

    def _map_symbol(self, lead_symbol: str) -> str | None:
        """lead 品种 → follower 品种；空 map = 恒等；非空缺项 = None（跳过 + 告警）。"""
        if not self._symbol_map:
            return lead_symbol
        return self._symbol_map.get(lead_symbol)

    def _scale_qty(self, intent: OrderIntent, spec: Any) -> Decimal:
        """按 scale_mode 缩放并量化到 qty_step。

        fixed：``qty × scale``；balance：``qty × follower_balance/lead_balance``
        （缺 lead_balance / 无 balance_provider → 回落 fixed）。
        """
        base = intent.qty or Decimal("0")
        if self._scale_mode == "balance":
            lb = intent.lead_balance
            fb = self._follower_balance()
            if lb and fb and lb > 0:
                qty = base * (fb / lb)
            else:
                qty = base * self._scale
        else:
            qty = base * self._scale
        step = getattr(spec, "qty_step", None) if spec is not None else None
        if step and step > 0:
            try:
                qty = (qty / step).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * step
            except (InvalidOperation, ArithmeticError):
                pass
        return qty

    def _follower_balance(self) -> Decimal | None:
        if self._balance_provider is None:
            return None
        try:
            v = self._balance_provider()
            return None if v is None else Decimal(str(v))
        except Exception as e:  # pragma: no cover - 余额查询失败回落 fixed
            logger.warning(f"[AGENT] balance_provider error: {e}")
            return None

    def _execute_intent(self, intent: OrderIntent) -> Report:
        """执行一条 intent → 生成 Report（幂等去重 + 缩放 + 映射 + 本地执行）。"""
        coid = intent.client_order_id
        # ① 幂等去重：已处理 → DUPLICATE，不再执行
        if self._dedup.seen(coid):
            return Report(coid, self._follower, "DUPLICATE", reason="already processed")

        action = intent.action
        try:
            if action == ACTION_FLATTEN:
                report = self._do_flatten(intent)
            elif action == ACTION_CANCEL:
                report = self._do_cancel(intent)
            else:
                report = self._do_order(intent)
        except Exception as e:
            logger.error(f"[AGENT] execute error coid={coid}: {e}", exc_info=True)
            report = Report(coid, self._follower, "DEAD", reason=f"execute error: {e}")

        self._dedup.mark(coid, report.status)
        return report

    def _do_order(self, intent: OrderIntent) -> Report:
        coid = intent.client_order_id
        # 心跳丢失 → OPEN hold（只减风险：CLOSE 放行）
        if self._lead_lost and intent.offset == Offset.OPEN.value:
            logger.warning(f"[AGENT] hold OPEN coid={coid}: lead_lost")
            return Report(coid, self._follower, "REJECT", reason="lead_lost: hold")

        mapped = self._map_symbol(intent.symbol)
        if mapped is None:
            self._alert_sync(f"no symbol_map for lead symbol {intent.symbol!r} → skipped")
            return Report(coid, self._follower, "SKIPPED",
                          reason=f"no symbol_map for {intent.symbol}")

        spec = self._specs.get(mapped)
        if spec is None:
            self._alert_sync(f"no spec for mapped symbol {mapped!r} → skipped")
            return Report(coid, self._follower, "SKIPPED", reason=f"no spec for {mapped}")

        qty = self._scale_qty(intent, spec)
        if qty <= 0:
            return Report(coid, self._follower, "SKIPPED",
                          reason=f"scaled qty rounds to zero (lead={intent.qty})")

        try:
            side = OrderSide(intent.side)
            offset = Offset(intent.offset)
            kind = OrderKind(intent.kind) if intent.kind else OrderKind.MARKET
        except ValueError as e:
            return Report(coid, self._follower, "REJECT", reason=f"bad enum: {e}")
        tif = None
        if intent.tif:
            try:
                tif = Tif(intent.tif)
            except ValueError:
                tif = None

        res = self._api.send_order(
            side, offset, qty, kind=kind, price=intent.price,
            stop_price=intent.stop_price, sl=intent.sl, tp=intent.tp,
            tif=tif, symbol=mapped,
        )
        return self._report_from_result(coid, res, qty)

    def _do_flatten(self, intent: OrderIntent) -> Report:
        # flatten 永远放行（只减风险），即使 lead_lost
        coid = intent.client_order_id
        mapped = self._map_symbol(intent.symbol)
        if mapped is None:
            self._alert_sync(f"no symbol_map for lead symbol {intent.symbol!r} → skip flatten")
            return Report(coid, self._follower, "SKIPPED",
                          reason=f"no symbol_map for {intent.symbol}")
        info = self._api.flatten(symbol=mapped)
        close = info.get("close") if isinstance(info, dict) else None
        net_vol = info.get("net_vol") if isinstance(info, dict) else None
        if close is None:
            return Report(coid, self._follower, "FILLED", filled_qty=Decimal("0"),
                          reason=f"flatten: no net position (net_vol={net_vol})")
        return self._report_from_result(coid, close, abs(net_vol or Decimal("0")),
                                        prefix="flatten: ")

    def _do_cancel(self, intent: OrderIntent) -> Report:
        coid = intent.client_order_id
        mapped = self._map_symbol(intent.symbol)
        if mapped is None:
            self._alert_sync(f"no symbol_map for lead symbol {intent.symbol!r} → skip cancel")
            return Report(coid, self._follower, "SKIPPED",
                          reason=f"no symbol_map for {intent.symbol}")
        n = self._api.cancel_all(symbol=mapped)
        return Report(coid, self._follower, "FILLED", filled_qty=Decimal("0"),
                      reason=f"cancel_all: canceled={n}")

    def _report_from_result(self, coid: str, res: Any, qty: Decimal,
                            prefix: str = "") -> Report:
        """KqApi OrderResult → Report（FILLED / IN_FLIGHT / REJECT）。"""
        ok = bool(getattr(res, "ok", False))
        filled_qty = getattr(res, "filled_qty", Decimal("0")) or Decimal("0")
        filled_price = getattr(res, "filled_price", None)
        reason = getattr(res, "reason", "") or ""
        if ok and filled_qty and filled_qty > 0:
            status = "FILLED"
        elif ok:
            status = "IN_FLIGHT"
        else:
            status = "REJECT"
        return Report(coid, self._follower, status, filled_qty=filled_qty,
                      filled_price=filled_price, reason=f"{prefix}{reason}",
                      )

    def _alert_sync(self, message: str) -> None:
        """同步上下文（to_thread 内）触发告警：无 alert 则仅日志。"""
        logger.warning(f"[AGENT] {message}")
        if self._alert is None:
            return
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(self._alert_warn(message), loop)
        except Exception:  # pragma: no cover - 告警失败不阻断执行
            pass
