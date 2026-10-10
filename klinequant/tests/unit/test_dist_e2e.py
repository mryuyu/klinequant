"""Phase 3 端到端单测：in-process 真 ZMQ，lead broadcaster → OrderAgent → report 回 lead。

全链路断言：lead 广播下单意图 → follower（假执行器）按 scale/品种映射跟单 →
回报经反向扇入回流 lead 聚合；重放同一 coid → follower 幂等（DUPLICATE 回报，不重复执行）。
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from protocol.types import OrderResult
from strategy.sdk.broadcaster import SignalBroadcaster
from strategy.sdk.order_agent import OrderAgent

TOKEN = "e2e-token"
INTENT_EP = "tcp://127.0.0.1:16680"
REPORT_EP = "tcp://127.0.0.1:16681"


class FakeApi:
    def __init__(self):
        self.calls: list[dict] = []
        self.flatten_calls: list = []

    def send_order(self, side, offset, qty, **kw):
        self.calls.append({"side": side, "offset": offset, "qty": qty, **kw})
        return OrderResult(ok=True, filled_qty=qty, filled_price=Decimal("60000"))

    def flatten(self, symbol=None, period=None):
        self.flatten_calls.append(symbol)
        return {"canceled": 0, "net_vol": Decimal("0"), "close": None}

    def cancel_all(self, symbol=None, period=None):
        return 0


async def _wait_for(pred, timeout=4.0, interval=0.02):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


async def test_lead_to_follower_to_report_roundtrip(tmp_path):
    collected: list = []
    api = FakeApi()
    bcast = SignalBroadcaster(
        token=TOKEN, account="lead-1", intent_endpoint=INTENT_EP,
        report_endpoint=REPORT_EP, heartbeat_interval=0, report_callback=collected.append,
    )
    agent = OrderAgent(
        token=TOKEN, follower_account="follower-1", scale=Decimal("0.5"),
        symbol_map={"BTCUSDT": "BTCUSDT.P"}, api=api,
        specs={"BTCUSDT.P": SimpleNamespace(qty_step=Decimal("0.01"))},
        lead_intent_endpoint=INTENT_EP, report_endpoint=REPORT_EP,
        dedup_path=tmp_path / "e2e.db", lead_timeout=30.0,
    )
    try:
        bcast.start()               # lead：intent PUB bind + report SUB bind
        await agent.start_async()   # follower：intent SUB connect + report PUB connect
        await asyncio.sleep(0.6)    # 等 ZMQ 连接建立（避免 slow-joiner 丢包）

        # lead 广播一笔下单意图（qty=2）
        bcast.broadcast_order(
            client_order_id="KQ-lead-1m-1-aaaa", tag="1m", symbol="BTCUSDT",
            side="BUY", offset="OPEN", qty=Decimal("2"), kind="MARKET",
        )

        # follower 应跟单：映射品种 + 缩放 qty（2×0.5=1）
        await _wait_for(lambda: len(api.calls) >= 1)
        call = api.calls[0]
        assert call["symbol"] == "BTCUSDT.P"
        assert call["qty"] == Decimal("1")

        # 回报应回流 lead 聚合
        await _wait_for(lambda: len(collected) >= 1)
        rep = collected[0]
        assert rep.client_order_id == "KQ-lead-1m-1-aaaa"
        assert rep.follower == "follower-1"
        assert rep.status == "FILLED"
        assert rep.filled_qty == Decimal("1")

        # 重放同一 coid → follower 幂等：不重复执行，回报 DUPLICATE
        bcast.broadcast_order(
            client_order_id="KQ-lead-1m-1-aaaa", tag="1m", symbol="BTCUSDT",
            side="BUY", offset="OPEN", qty=Decimal("2"), kind="MARKET",
        )
        await _wait_for(lambda: len(collected) >= 2)
        assert len(api.calls) == 1  # 未再次执行
        statuses = {r.status for r in collected}
        assert "DUPLICATE" in statuses
    finally:
        await agent.stop_async()
        bcast.stop()


async def test_flatten_intent_executes_on_follower(tmp_path):
    collected: list = []
    api = FakeApi()
    bcast = SignalBroadcaster(
        token=TOKEN, account="lead-1",
        intent_endpoint="tcp://127.0.0.1:16682",
        report_endpoint="tcp://127.0.0.1:16683",
        heartbeat_interval=0, report_callback=collected.append,
    )
    agent = OrderAgent(
        token=TOKEN, follower_account="follower-1", api=api,
        specs={"BTCUSDT.P": SimpleNamespace(qty_step=Decimal("0.01"))},
        symbol_map={"BTCUSDT": "BTCUSDT.P"},
        lead_intent_endpoint="tcp://127.0.0.1:16682",
        report_endpoint="tcp://127.0.0.1:16683",
        dedup_path=tmp_path / "e2e_flat.db", lead_timeout=30.0,
    )
    try:
        bcast.start()
        await agent.start_async()
        await asyncio.sleep(0.6)

        bcast.broadcast_flatten(client_order_id="flatten-BTCUSDT-1", tag="1m", symbol="BTCUSDT")

        await _wait_for(lambda: len(api.flatten_calls) >= 1)
        assert api.flatten_calls[0] == "BTCUSDT.P"
        await _wait_for(lambda: len(collected) >= 1)
        assert collected[0].status == "FILLED"
        assert "no net position" in collected[0].reason
    finally:
        await agent.stop_async()
        bcast.stop()
