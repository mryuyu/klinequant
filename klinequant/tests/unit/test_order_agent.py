"""Phase 3 follower 侧 OrderAgent 单测（假执行器 + 本机 dedup，不依赖开市/ZMQ）。

覆盖：
  ① 同一 lead coid 重放 → 只执行一次（dedup 落盘，重开 agent 仍幂等）
  ② fixed 缩放 + balance 缩放数值正确并量化到 step
  ③ symbol_map 命中/缺失（缺失跳过不执行）
  ④ 心跳超时 → OPEN hold、CLOSE/flatten 放行
  ⑤ 非白名单消息 / token 不匹配 → 拒绝执行
  ⑥ 回报状态正确（FILLED / IN_FLIGHT / REJECT / DEAD / SKIPPED / DUPLICATE）
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from types import SimpleNamespace

from protocol.messages import Message
from protocol.types import Offset, OrderResult, OrderSide
from strategy.sdk.dist_protocol import (
    ACTION_CANCEL,
    ACTION_FLATTEN,
    ACTION_ORDER,
    OrderIntent,
    intent_to_message,
)
from strategy.sdk.order_agent import AgentDedup, OrderAgent

TOKEN = "agent-token"


def _wait_for(pred, timeout=2.0, interval=0.01):
    async def _inner():
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if pred():
                return
            await asyncio.sleep(interval)
        raise AssertionError("condition not met within timeout")
    return _inner()


class FakeApi:
    """KqApi 兼容桩：记录调用，返回可配置的 OrderResult / flatten dict。"""

    def __init__(self, order_result=None, flatten_info=None, cancel_count=0):
        self.calls: list[dict] = []
        self.flatten_calls: list = []
        self.cancel_calls: list = []
        self._order_result = (
            order_result if order_result is not None
            else OrderResult(ok=True, filled_qty=Decimal("1"), filled_price=Decimal("100"))
        )
        self._flatten_info = (
            flatten_info if flatten_info is not None
            else {"canceled": 0, "net_vol": Decimal("1"),
                  "close": OrderResult(ok=True, filled_qty=Decimal("1"),
                                       filled_price=Decimal("100"))}
        )
        self._cancel_count = cancel_count

    def send_order(self, side, offset, qty, **kw):
        self.calls.append({"side": side, "offset": offset, "qty": qty, **kw})
        return self._order_result

    def flatten(self, symbol=None, period=None):
        self.flatten_calls.append(symbol)
        return self._flatten_info

    def cancel_all(self, symbol=None, period=None):
        self.cancel_calls.append(symbol)
        return self._cancel_count


def make_agent(tmp_path, *, scale=Decimal("1"), scale_mode="fixed", symbol_map=None,
               api=None, specs=None, dedup_name="dedup.db", lead_timeout=10.0,
               balance_provider=None):
    specs = specs if specs is not None else {
        "BTCUSDT": SimpleNamespace(qty_step=Decimal("0.01"))
    }
    api = api if api is not None else FakeApi()
    agent = OrderAgent(
        token=TOKEN, follower_account="follower-1", scale=scale, scale_mode=scale_mode,
        symbol_map=symbol_map, api=api, specs=specs,
        dedup_path=tmp_path / dedup_name, lead_timeout=lead_timeout,
        balance_provider=balance_provider,
    )
    return agent, api


def _open_intent(coid, symbol="BTCUSDT", qty=Decimal("1")):
    return OrderIntent(client_order_id=coid, symbol=symbol, side="BUY",
                       offset="OPEN", qty=qty, kind="MARKET", action=ACTION_ORDER)


# ═══════════════════════════════════════════
# ① 幂等去重
# ═══════════════════════════════════════════
def test_dedup_executes_once(tmp_path):
    agent, api = make_agent(tmp_path)
    intent = _open_intent("c1")
    r1 = agent._execute_intent(intent)
    r2 = agent._execute_intent(intent)
    assert len(api.calls) == 1
    assert r1.status == "FILLED"
    assert r2.status == "DUPLICATE"


def test_dedup_persists_across_restart(tmp_path):
    agent1, api1 = make_agent(tmp_path, dedup_name="persist.db")
    intent = _open_intent("cX")
    agent1._execute_intent(intent)
    assert len(api1.calls) == 1
    agent1._dedup.close()

    # 重开 agent，复用同一 dedup 库 → 仍幂等
    agent2, api2 = make_agent(tmp_path, dedup_name="persist.db")
    r = agent2._execute_intent(intent)
    assert r.status == "DUPLICATE"
    assert len(api2.calls) == 0


def test_agent_dedup_seen_mark(tmp_path):
    d = AgentDedup(tmp_path / "x.db")
    assert not d.seen("a")
    d.mark("a", "FILLED")
    assert d.seen("a")
    d.close()


# ═══════════════════════════════════════════
# ② 缩放
# ═══════════════════════════════════════════
def test_scale_fixed(tmp_path):
    agent, _ = make_agent(tmp_path, scale=Decimal("0.5"))
    spec = SimpleNamespace(qty_step=Decimal("0.01"))
    assert agent._scale_qty(OrderIntent(client_order_id="s", qty=Decimal("3")), spec) \
        == Decimal("1.5")


def test_scale_fixed_rounds_to_step(tmp_path):
    agent, _ = make_agent(tmp_path, scale=Decimal("1"))
    spec = SimpleNamespace(qty_step=Decimal("0.1"))
    # 1.05 / 0.1 = 10.5 → ROUND_HALF_UP → 11 → 1.1
    assert agent._scale_qty(OrderIntent(client_order_id="s", qty=Decimal("1.05")), spec) \
        == Decimal("1.1")


def test_scale_balance_ratio(tmp_path):
    agent, _ = make_agent(
        tmp_path, scale_mode="balance", balance_provider=lambda: Decimal("500"))
    spec = SimpleNamespace(qty_step=Decimal("0.01"))
    intent = OrderIntent(client_order_id="b", qty=Decimal("2"), lead_balance=Decimal("1000"))
    # 500/1000 = 0.5 → 2*0.5 = 1
    assert agent._scale_qty(intent, spec) == Decimal("1")


def test_scale_balance_falls_back_to_fixed(tmp_path):
    # 缺 lead_balance → 回落 fixed scale
    agent, _ = make_agent(
        tmp_path, scale=Decimal("0.5"), scale_mode="balance",
        balance_provider=lambda: Decimal("500"))
    spec = SimpleNamespace(qty_step=Decimal("0.01"))
    intent = OrderIntent(client_order_id="b", qty=Decimal("2"))  # 无 lead_balance
    assert agent._scale_qty(intent, spec) == Decimal("1")


def test_scaled_qty_zero_skipped(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(
        tmp_path, scale=Decimal("0.001"), api=api,
        specs={"BTCUSDT": SimpleNamespace(qty_step=Decimal("1"))})
    r = agent._execute_intent(_open_intent("z1", qty=Decimal("0.1")))
    assert r.status == "SKIPPED"
    assert len(api.calls) == 0


# ═══════════════════════════════════════════
# ③ 品种映射
# ═══════════════════════════════════════════
def test_symbol_map_hit(tmp_path):
    agent, _ = make_agent(tmp_path, symbol_map={"BTCUSDT": "BTCUSDT.P"})
    assert agent._map_symbol("BTCUSDT") == "BTCUSDT.P"


def test_symbol_map_identity_when_empty(tmp_path):
    agent, _ = make_agent(tmp_path, symbol_map=None)
    assert agent._map_symbol("BTCUSDT") == "BTCUSDT"


def test_symbol_map_miss_skips_execution(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(
        tmp_path, symbol_map={"ETHUSDT": "ETH"}, api=api,
        specs={"ETH": SimpleNamespace(qty_step=Decimal("0.01"))})
    r = agent._execute_intent(_open_intent("m1", symbol="BTCUSDT"))
    assert r.status == "SKIPPED"
    assert len(api.calls) == 0


def test_order_executes_with_mapped_symbol_and_scaled_qty(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(
        tmp_path, scale=Decimal("0.5"), symbol_map={"BTCUSDT": "BTCUSDT.P"}, api=api,
        specs={"BTCUSDT.P": SimpleNamespace(qty_step=Decimal("0.01"))})
    r = agent._execute_intent(_open_intent("e1", symbol="BTCUSDT", qty=Decimal("2")))
    assert r.status == "FILLED"
    call = api.calls[0]
    assert call["symbol"] == "BTCUSDT.P"
    assert call["qty"] == Decimal("1")       # 2 × 0.5
    assert call["side"] == OrderSide.BUY
    assert call["offset"] == Offset.OPEN


# ═══════════════════════════════════════════
# ④ 心跳 hold
# ═══════════════════════════════════════════
def test_lead_lost_holds_open(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(tmp_path, api=api)
    agent._lead_lost = True
    r = agent._execute_intent(_open_intent("h1"))
    assert r.status == "REJECT"
    assert "lead_lost" in r.reason
    assert len(api.calls) == 0


def test_lead_lost_allows_close(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(tmp_path, api=api)
    agent._lead_lost = True
    close = OrderIntent(client_order_id="h2", symbol="BTCUSDT", side="SELL",
                        offset="CLOSE", qty=Decimal("1"), action=ACTION_ORDER)
    r = agent._execute_intent(close)
    assert r.status == "FILLED"
    assert len(api.calls) == 1


def test_lead_lost_allows_flatten(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(tmp_path, api=api)
    agent._lead_lost = True
    flat = OrderIntent(client_order_id="h3", symbol="BTCUSDT", action=ACTION_FLATTEN)
    r = agent._execute_intent(flat)
    assert r.status == "FILLED"
    assert api.flatten_calls == ["BTCUSDT"]


async def test_heartbeat_monitor_sets_lead_lost(tmp_path):
    agent, _ = make_agent(tmp_path, lead_timeout=0.5)
    agent._last_hb = time.monotonic() - 1.0  # 已陈旧
    task = asyncio.get_event_loop().create_task(agent._heartbeat_monitor())
    try:
        await _wait_for(lambda: agent._lead_lost)
    finally:
        task.cancel()


async def test_heartbeat_recovery(tmp_path):
    agent, _ = make_agent(tmp_path, lead_timeout=0.5)
    agent._last_hb = time.monotonic() - 1.0
    task = asyncio.get_event_loop().create_task(agent._heartbeat_monitor())
    try:
        await _wait_for(lambda: agent._lead_lost)
        agent._last_hb = time.monotonic()  # 心跳恢复
        await _wait_for(lambda: not agent._lead_lost)
    finally:
        task.cancel()


# ═══════════════════════════════════════════
# ⑤ 信任边界：非白名单 / token 不符
# ═══════════════════════════════════════════
async def test_non_whitelist_rejected(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(tmp_path, api=api)
    evil = Message(msg_type="EXEC_STRATEGY", source="attacker",
                   payload={"token": TOKEN, "code": "<malicious>"})
    await agent._on_intent_msg(evil)
    assert len(api.calls) == 0


async def test_token_mismatch_rejected(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(tmp_path, api=api)
    msg = intent_to_message(_open_intent("t1"), "wrong-token")
    await agent._on_intent_msg(msg)
    assert len(api.calls) == 0


async def test_valid_intent_executes_via_handler(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(tmp_path, api=api)
    msg = intent_to_message(_open_intent("v1"), TOKEN)
    await agent._on_intent_msg(msg)
    assert len(api.calls) == 1


# ═══════════════════════════════════════════
# ⑥ 回报状态
# ═══════════════════════════════════════════
def test_report_reject_when_order_fails(tmp_path):
    api = FakeApi(order_result=OrderResult(ok=False, reason="insufficient margin"))
    agent, _ = make_agent(tmp_path, api=api)
    r = agent._execute_intent(_open_intent("r1"))
    assert r.status == "REJECT"
    assert "margin" in r.reason


def test_report_in_flight_when_ok_no_fill(tmp_path):
    api = FakeApi(order_result=OrderResult(ok=True, filled_qty=Decimal("0")))
    agent, _ = make_agent(tmp_path, api=api)
    r = agent._execute_intent(_open_intent("r2"))
    assert r.status == "IN_FLIGHT"


def test_flatten_report(tmp_path):
    api = FakeApi()
    agent, _ = make_agent(tmp_path, api=api)
    r = agent._execute_intent(
        OrderIntent(client_order_id="fl1", symbol="BTCUSDT", action=ACTION_FLATTEN))
    assert r.status == "FILLED"
    assert api.flatten_calls == ["BTCUSDT"]


def test_cancel_report(tmp_path):
    api = FakeApi(cancel_count=2)
    agent, _ = make_agent(tmp_path, api=api)
    r = agent._execute_intent(
        OrderIntent(client_order_id="cn1", symbol="BTCUSDT", action=ACTION_CANCEL))
    assert r.status == "FILLED"
    assert "canceled=2" in r.reason
    assert api.cancel_calls == ["BTCUSDT"]


def test_execute_error_reports_dead(tmp_path):
    class Boom(FakeApi):
        def send_order(self, *a, **k):
            raise RuntimeError("venue down")

    agent, _ = make_agent(tmp_path, api=Boom())
    r = agent._execute_intent(_open_intent("d1"))
    assert r.status == "DEAD"
    assert "venue down" in r.reason
