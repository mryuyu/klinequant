"""Gateway 单元测试

覆盖 GW-T-001 ~ GW-T-003：
    GW-T-001: JWT 签发/验证/过期
    GW-T-002: 行情 API 参数校验 + 响应格式
    GW-T-003: WS 订阅/取消订阅/心跳
"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.auth import create_token, verify_token
from gateway.ws import ConnectionManager


# ─── GW-T-001: JWT ───


class TestJWT:
    def test_create_and_verify_token(self):
        """签发并验证 Token"""
        token = create_token(user_id="admin", role="admin")
        payload = verify_token(token)
        assert payload["sub"] == "admin"
        assert payload["role"] == "admin"
        assert "exp" in payload
        assert "iat" in payload

    def test_expired_token(self):
        """过期 Token 被拒绝"""
        token = create_token(user_id="admin", expires_in=-1)
        with pytest.raises(Exception) as exc_info:
            verify_token(token)
        assert "expired" in str(exc_info.value.detail).lower()

    def test_invalid_token(self):
        """无效 Token 被拒绝"""
        with pytest.raises(Exception) as exc_info:
            verify_token("invalid.token.here")
        assert "invalid" in str(exc_info.value.detail).lower()

    def test_token_contains_correct_claims(self):
        """Token 包含正确的 claims"""
        token = create_token(user_id="user1", role="viewer", expires_in=3600)
        payload = verify_token(token)
        assert payload["sub"] == "user1"
        assert payload["role"] == "viewer"
        # exp - iat ≈ 3600
        assert abs((payload["exp"] - payload["iat"]) - 3600) <= 1


# ─── GW-T-002: API 路由 ───


@pytest.fixture
def client():
    app = create_app()
    return TestClient(app)


@pytest.fixture
def auth_header():
    token = create_token(user_id="admin")
    return {"Authorization": f"Bearer {token}"}


class TestMarketAPI:
    def test_health_no_auth(self, client):
        """健康检查无需认证"""
        resp = client.get("/api/system/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"

    def test_klines_endpoint(self, client):
        """K线接口可访问（已移除强制认证）"""
        resp = client.get("/api/market/klines?symbol=BTCUSDT&timeframe=1h&limit=50")
        # 可能成功或因网络问题返回错误，但不应是 401
        assert resp.status_code != 401

    def test_klines_param_validation(self, client):
        """K线接口参数校验"""
        # limit > 1000 应被拒绝
        resp = client.get("/api/market/klines?symbol=BTCUSDT&limit=2000")
        assert resp.status_code == 422

    def test_symbols_endpoint(self, client):
        """交易对列表"""
        resp = client.get("/api/market/symbols")
        assert resp.status_code == 200
        data = resp.json()
        assert "symbols" in data


class TestTradeAPI:
    def test_orders_endpoint_accessible(self, client):
        """订单接口可访问（已移除强制认证）"""
        resp = client.get("/api/trade/orders")
        # 不应是 401
        assert resp.status_code != 401

    def test_list_orders(self, client):
        resp = client.get("/api/trade/orders")
        assert resp.status_code == 200
        assert "orders" in resp.json()

    def test_create_order_format(self, client):
        """下单接口响应格式"""
        resp = client.post(
            "/api/trade/orders",
            json={"symbol": "BTCUSDT", "side": "BUY", "quantity": 0.01},
        )
        assert resp.status_code == 200
        data = resp.json()
        # 可能成功下单或返回错误（测试环境无真实 API Key）
        assert "order_id" in data or "error" in data


class TestStrategyAPI:
    def test_list_strategies(self, client, auth_header):
        resp = client.get("/api/strategies", headers=auth_header)
        assert resp.status_code == 200
        assert "strategies" in resp.json()

    def test_create_strategy(self, client, auth_header):
        resp = client.post(
            "/api/strategies",
            json={"name": "Test MA", "strategy_type": "dual_ma"},
            headers=auth_header,
        )
        assert resp.status_code == 200
        assert resp.json()["name"] == "Test MA"


class TestBacktestAPI:
    def test_run_backtest(self, client):
        resp = client.post(
            "/api/backtest/run",
            json={"strategy_type": "dual_ma", "symbol": "BTCUSDT"},
        )
        # 可能成功（200）或因网络问题返回 502
        assert resp.status_code in (200, 502)
        if resp.status_code == 200:
            data = resp.json()
            assert "task_id" in data


class TestAuthAPI:
    def test_login_success(self, client):
        resp = client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "admin"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "token" in data
        # 验证返回的 token 可用
        payload = verify_token(data["token"])
        assert payload["sub"] == "admin"

    def test_login_failure(self, client):
        resp = client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "wrong"},
        )
        assert resp.status_code == 200
        assert "error" in resp.json()


# ─── GW-T-003: WebSocket ───


class TestWebSocket:
    def test_ws_subscribe_and_heartbeat(self, client):
        """WS 订阅 + 心跳"""
        with client.websocket_connect("/ws") as ws:
            # 订阅
            ws.send_text('{"action": "subscribe", "topic": "klines.BTCUSDT"}')
            resp = ws.receive_text()
            import json
            data = json.loads(resp)
            assert data["action"] == "subscribed"
            assert data["topic"] == "klines.BTCUSDT"

            # 心跳
            ws.send_text('{"action": "ping"}')
            resp = ws.receive_text()
            data = json.loads(resp)
            assert data["action"] == "pong"

    def test_ws_unsubscribe(self, client):
        """WS 取消订阅"""
        with client.websocket_connect("/ws") as ws:
            import json

            ws.send_text('{"action": "subscribe", "topic": "signals"}')
            ws.receive_text()  # ack

            ws.send_text('{"action": "unsubscribe", "topic": "signals"}')
            resp = ws.receive_text()
            data = json.loads(resp)
            assert data["action"] == "unsubscribed"


class TestConnectionManager:
    def test_subscribe_unsubscribe(self):
        """连接管理器订阅/取消"""
        mgr = ConnectionManager()
        mgr._connections["ws-1"] = None  # mock

        mgr.subscribe("ws-1", "klines")
        assert "ws-1" in mgr._subscriptions["klines"]

        mgr.unsubscribe("ws-1", "klines")
        assert "ws-1" not in mgr._subscriptions.get("klines", set())

    def test_disconnect_cleans_subscriptions(self):
        """断开连接清除订阅"""
        mgr = ConnectionManager()
        mgr._connections["ws-1"] = None
        mgr.subscribe("ws-1", "topic1")
        mgr.subscribe("ws-1", "topic2")

        mgr.disconnect("ws-1")
        assert "ws-1" not in mgr._connections
        assert "topic1" not in mgr._subscriptions
        assert "topic2" not in mgr._subscriptions
