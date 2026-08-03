"""WebSocket 推送服务

功能：
    - 客户端连接管理
    - 主题订阅/取消订阅
    - K线/信号/订单/持仓 实时推送
    - 心跳检测

遵循需求文档 §4.7 GW-007。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, Set

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class ConnectionManager:
    """WebSocket 连接管理器"""

    def __init__(self):
        # ws_id -> WebSocket
        self._connections: Dict[str, WebSocket] = {}
        # topic -> set of ws_ids
        self._subscriptions: Dict[str, Set[str]] = {}
        self._counter = 0

    @property
    def active_connections(self) -> int:
        return len(self._connections)

    async def connect(self, websocket: WebSocket) -> str:
        """接受连接，返回连接 ID"""
        await websocket.accept()
        self._counter += 1
        ws_id = f"ws-{self._counter}"
        self._connections[ws_id] = websocket
        logger.info(f"WS connected: {ws_id} (total={len(self._connections)})")
        return ws_id

    def disconnect(self, ws_id: str) -> None:
        """断开连接"""
        self._connections.pop(ws_id, None)
        # 清除订阅
        for topic in list(self._subscriptions.keys()):
            self._subscriptions[topic].discard(ws_id)
            if not self._subscriptions[topic]:
                del self._subscriptions[topic]
        logger.info(f"WS disconnected: {ws_id}")

    def subscribe(self, ws_id: str, topic: str) -> None:
        """订阅主题"""
        if topic not in self._subscriptions:
            self._subscriptions[topic] = set()
        self._subscriptions[topic].add(ws_id)

    def unsubscribe(self, ws_id: str, topic: str) -> None:
        """取消订阅"""
        if topic in self._subscriptions:
            self._subscriptions[topic].discard(ws_id)

    async def publish(self, topic: str, data: Any) -> int:
        """向主题的所有订阅者推送数据

        Returns:
            成功推送的连接数
        """
        subscribers = self._subscriptions.get(topic, set())
        if not subscribers:
            return 0

        message = json.dumps({
            "topic": topic,
            "data": data,
            "timestamp": int(time.time() * 1000),
        })

        sent = 0
        dead = []
        for ws_id in subscribers:
            ws = self._connections.get(ws_id)
            if ws is None:
                dead.append(ws_id)
                continue
            try:
                await ws.send_text(message)
                sent += 1
            except Exception:
                dead.append(ws_id)

        for ws_id in dead:
            self.disconnect(ws_id)

        return sent

    async def handle_message(self, ws_id: str, raw: str) -> None:
        """处理客户端消息（订阅/取消订阅/心跳）"""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        action = msg.get("action")
        topic = msg.get("topic")

        if action == "subscribe" and topic:
            self.subscribe(ws_id, topic)
            await self._send_ack(ws_id, "subscribed", topic)
        elif action == "unsubscribe" and topic:
            self.unsubscribe(ws_id, topic)
            await self._send_ack(ws_id, "unsubscribed", topic)
        elif action == "ping":
            await self._send_ack(ws_id, "pong", None)

    async def _send_ack(self, ws_id: str, action: str, topic: Any) -> None:
        ws = self._connections.get(ws_id)
        if ws:
            await ws.send_text(json.dumps({"action": action, "topic": topic}))


# 全局单例
ws_manager = ConnectionManager()
