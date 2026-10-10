"""Phase 3 lead 侧 SignalBroadcaster — 订单意图分发 + 回报聚合。

职责（信任边界的 lead 端）：
  - 把本地策略产生的**白名单指令**（order/cancel/flatten）经 ZMQ PUB 广播给 followers；
  - 周期发心跳（DIST_HEARTBEAT），供 follower 判定 lead 存活；
  - 订阅 followers 的执行回报（DIST_REPORT，反向扇入：lead SUB **bind**）→ 聚合写日志
    + 可选复用 ``core.notification.AlertManager`` 告警。

线程模型：LiveRunner/KqApi 在拓扑 Z 的品种工作线程里**同步**调用本类，而 ZMQ 栈是
async → 本类自带一个 daemon 线程跑独立 ``SelectorEventLoop``（Windows ZMQ 必需），
``publish_*`` 经 ``run_coroutine_threadsafe`` **fire-and-forget** 提交，绝不阻塞、绝不
把分发异常抛回本地策略执行（分发是 best-effort，本地成交才是 lead 的主真相）。

seq/幂等：intent 携带 lead 侧 WAL 生成的 ``client_order_id``（内嵌 next_seq、重启续号），
本类**不再单独消费 next_seq**（避免与 OrderIdFactory 双递增）。
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING

from protocol.transport.zmq_transport import ZmqTransport
from strategy.sdk.dist_protocol import (
    ACTION_CANCEL,
    ACTION_FLATTEN,
    ACTION_ORDER,
    DEFAULT_INTENT_ENDPOINT,
    DEFAULT_REPORT_ENDPOINT,
    TOPIC_HEARTBEAT,
    TOPIC_INTENT,
    TOPIC_REPORT,
    OrderIntent,
    Report,
    heartbeat_message,
    intent_to_message,
    parse_endpoint,
    parse_message,
    report_to_message,  # noqa: F401  (再导出便于测试/复用)
)

if TYPE_CHECKING:  # 仅类型标注，避免运行期耦合 notification
    from core.notification.alert_manager import AlertManager

logger = logging.getLogger(__name__)

__all__ = ["SignalBroadcaster", "build_broadcaster_from_account"]


class SignalBroadcaster:
    """lead 侧信号广播器（进程内单例，LiveRunner 注入到每个 KqApi）。

    Args:
        token: 分发通道鉴权 token（明文只在 .env；lead/follower 必须一致）。
        account: lead 账户名（写入 intent.account 供 follower 上下文/日志）。
        intent_endpoint: intent/heartbeat 的 PUB **bind** 端点（followers connect）。
        report_endpoint: report 的 SUB **bind** 端点（followers 的 report PUB connect）。
        alert_manager: 可选告警管理器；None 时回报仅写日志。
        heartbeat_interval: 心跳间隔秒（<=0 关闭心跳）。
        report_callback: 可选 ``Callable[[Report], None]``，每条回报回调（测试/二次消费）。
        source: Message.source 标识（默认 "lead"）。
    """

    def __init__(
        self,
        *,
        token: str,
        account: str = "",
        intent_endpoint: str = DEFAULT_INTENT_ENDPOINT,
        report_endpoint: str = DEFAULT_REPORT_ENDPOINT,
        alert_manager: AlertManager | None = None,
        heartbeat_interval: float = 2.0,
        report_callback: Callable[[Report], None] | None = None,
        source: str = "lead",
    ):
        self._token = token
        self._account = account
        self._intent_host, self._intent_port = parse_endpoint(intent_endpoint)
        self._report_host, self._report_port = parse_endpoint(report_endpoint)
        self._alert = alert_manager
        self._heartbeat_interval = heartbeat_interval
        self._report_callback = report_callback
        self._source = source

        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._pub: ZmqTransport | None = None       # intent/heartbeat PUB (bind)
        self._sub: ZmqTransport | None = None       # report SUB (bind, 扇入)
        self._hb_task: asyncio.Task | None = None
        self._started = False
        self._lock = threading.Lock()

    # ─── 生命周期 ───

    def start(self) -> None:
        """启动后台事件循环线程 + 绑定 PUB/SUB（同步，阻塞至就绪）。"""
        with self._lock:
            if self._started:
                return
            self._thread = threading.Thread(
                target=self._loop_runner, daemon=True, name="signal-broadcaster"
            )
            self._thread.start()
        # 等循环就绪后再提交启动协程
        if not self._ready.wait(timeout=10.0):
            raise RuntimeError("SignalBroadcaster event loop did not start in time")
        asyncio.run_coroutine_threadsafe(self._async_start(), self._loop).result(timeout=10.0)
        with self._lock:
            self._started = True
        logger.info(
            f"[DIST] SignalBroadcaster started (account={self._account or '<none>'} "
            f"intent=tcp://{self._intent_host}:{self._intent_port} "
            f"report=tcp://{self._report_host}:{self._report_port})"
        )

    def _loop_runner(self) -> None:
        # Windows ZMQ asyncio 必须 SelectorEventLoop（Proactor 不支持 add_reader）
        if sys.platform == "win32":
            self._loop = asyncio.SelectorEventLoop()
        else:
            self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.call_soon_threadsafe(self._ready.set)
        self._loop.run_forever()

    async def _async_start(self) -> None:
        self._pub = ZmqTransport(
            role="publisher", bind_host=self._intent_host,
            pub_port=self._intent_port, pub_bind=True,
        )
        await self._pub.start()
        self._sub = ZmqTransport(
            role="subscriber", bind_host=self._report_host,
            sub_port=self._report_port, sub_bind=True,
        )
        await self._sub.start()
        await self._sub.subscribe(TOPIC_REPORT, self._on_report)
        if self._heartbeat_interval > 0:
            self._hb_task = self._loop.create_task(self._heartbeat_loop())

    def stop(self) -> None:
        """停止心跳 + 关闭 PUB/SUB + 收尾事件循环（best-effort，异常不阻断退出）。"""
        with self._lock:
            if not self._started:
                return
            self._started = False
        try:
            if self._loop is not None and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._async_stop(), self._loop
                ).result(timeout=10.0)
                self._loop.call_soon_threadsafe(self._loop.stop)
                if self._thread is not None:
                    self._thread.join(timeout=5)
        except Exception as e:  # pragma: no cover - 关停异常不阻断退出
            logger.warning(f"[DIST] SignalBroadcaster stop error: {e}")
        finally:
            if self._loop is not None:
                try:
                    self._loop.close()
                except Exception:
                    pass
                self._loop = None
        logger.info("[DIST] SignalBroadcaster stopped")

    async def _async_stop(self) -> None:
        if self._hb_task is not None and not self._hb_task.done():
            self._hb_task.cancel()
            try:
                await self._hb_task
            except asyncio.CancelledError:
                pass
        if self._sub is not None:
            await self._sub.stop()
        if self._pub is not None:
            await self._pub.stop()

    # ─── 广播（同步入口，供 KqApi 工作线程调用；fire-and-forget）───

    def publish_intent(self, intent: OrderIntent) -> None:
        """广播一条订单意图（best-effort：未启动/异常只记 warning，不抛回调用方）。"""
        if not self._started or self._loop is None or not self._loop.is_running():
            logger.warning("[DIST] publish_intent skipped: broadcaster not started")
            return
        intent.account = intent.account or self._account
        msg = intent_to_message(intent, self._token, source=self._source)
        # order/cancel/flatten 同走 intent topic（由 msg_type 区分 action）
        self._submit(self._pub.publish(TOPIC_INTENT, msg))

    def broadcast_order(
        self, *, client_order_id: str, tag: str, symbol: str, side: str, offset: str,
        qty: Decimal, kind: str = "MARKET", price: Decimal | None = None,
        stop_price: Decimal | None = None, sl: Decimal | None = None,
        tp: Decimal | None = None, tif: str = "",
        lead_balance: Decimal | None = None,
    ) -> None:
        """广播下单意图（send_order 每 leg 调用一次）。"""
        self.publish_intent(OrderIntent(
            client_order_id=client_order_id, tag=tag, symbol=symbol,
            side=side, offset=offset, qty=qty, kind=kind, price=price,
            stop_price=stop_price, sl=sl, tp=tp, tif=tif,
            action=ACTION_ORDER, lead_balance=lead_balance,
        ))

    def broadcast_cancel(self, *, client_order_id: str, tag: str, symbol: str) -> None:
        """广播撤挂单意图。"""
        self.publish_intent(OrderIntent(
            client_order_id=client_order_id, tag=tag, symbol=symbol, action=ACTION_CANCEL,
        ))

    def broadcast_flatten(self, *, client_order_id: str, tag: str, symbol: str) -> None:
        """广播一键清仓意图。"""
        self.publish_intent(OrderIntent(
            client_order_id=client_order_id, tag=tag, symbol=symbol, action=ACTION_FLATTEN,
        ))

    def _submit(self, coro) -> None:
        """把协程提交到后台循环（fire-and-forget），异常仅记日志。"""
        try:
            fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
            fut.add_done_callback(self._log_future_error)
        except Exception as e:  # pragma: no cover - 循环已停等竞态
            logger.warning(f"[DIST] submit failed: {e}")

    @staticmethod
    def _log_future_error(fut) -> None:
        try:
            exc = fut.exception()
        except (asyncio.CancelledError, Exception):  # noqa: B014 - 取异常本身可能抛
            return
        if exc is not None:
            logger.warning(f"[DIST] publish error: {exc}")

    # ─── 心跳 ───

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                try:
                    await self._pub.publish(
                        TOPIC_HEARTBEAT,
                        heartbeat_message(self._token, source=self._source),
                    )
                except Exception as e:  # pragma: no cover - 单次心跳失败不致命
                    logger.warning(f"[DIST] heartbeat publish error: {e}")
                await asyncio.sleep(self._heartbeat_interval)
        except asyncio.CancelledError:
            pass

    # ─── 回报聚合（async，跑在 broadcaster 循环上）───

    async def _on_report(self, msg) -> None:
        obj, reason = parse_message(msg, self._token)
        if obj is None:
            logger.warning(f"[DIST] report rejected: {reason}")
            await self._alert_warn(f"distribution report rejected: {reason}")
            return
        report: Report = obj
        logger.info(
            f"[DIST] report follower={report.follower} coid={report.client_order_id} "
            f"status={report.status} filled={report.filled_qty}@{report.filled_price} "
            f"reason={report.reason}"
        )
        if self._report_callback is not None:
            try:
                self._report_callback(report)
            except Exception as e:  # pragma: no cover - 回调异常不影响分发
                logger.warning(f"[DIST] report_callback error: {e}")
        if report.status in ("REJECT", "DEAD"):
            await self._alert_warn(
                f"follower {report.follower} order {report.status}: "
                f"{report.reason} (coid={report.client_order_id})"
            )

    async def _alert_warn(self, message: str) -> None:
        if self._alert is None:
            return
        try:
            await self._alert.fire_warning(message, source="signal_broadcaster")
        except Exception as e:  # pragma: no cover - 告警失败不阻断
            logger.warning(f"[DIST] alert fire error: {e}")


def build_broadcaster_from_account(
    account, alert_manager: AlertManager | None = None
) -> SignalBroadcaster | None:
    """从 :class:`~config.accounts.AccountConfig`（role==lead）构造 SignalBroadcaster。

    非 lead / account=None → 返回 None（standalone/follower 行为零变化）。读取
    ``account.extra`` 的 ``intent_endpoint`` / ``report_endpoint`` / ``token`` /
    ``heartbeat_interval``（缺省回落 DEFAULT_* / env ``KQ_LEAD_TOKEN`` / 2.0）。
    token 缺失 → 返回 None + 错误日志（无 token 无法鉴权，宁可不广播也不开无鉴权通道）。
    """
    if account is None or getattr(account, "role", "standalone") != "lead":
        return None
    extra = getattr(account, "extra", {}) or {}
    token = extra.get("token") or os.getenv("KQ_LEAD_TOKEN", "")
    if not token:
        logger.error(
            "[DIST] role=lead 但未配置 token（extra.token 或 env KQ_LEAD_TOKEN）；"
            "跳过分发（不开无鉴权通道）"
        )
        return None
    return SignalBroadcaster(
        token=token,
        account=account.name,
        intent_endpoint=extra.get("intent_endpoint", DEFAULT_INTENT_ENDPOINT),
        report_endpoint=extra.get("report_endpoint", DEFAULT_REPORT_ENDPOINT),
        alert_manager=alert_manager,
        heartbeat_interval=float(extra.get("heartbeat_interval", 2.0)),
    )
