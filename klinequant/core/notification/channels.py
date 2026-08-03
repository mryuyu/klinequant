"""通知渠道 — Webhook 集成

支持渠道：
    - 钉钉 (DingTalk) 机器人
    - 飞书 (Feishu/Lark) 机器人
    - Telegram Bot
    - 通用 Webhook

每个渠道实现统一接口，支持：
    - Markdown 格式消息
    - 签名验证（钉钉/飞书）
    - 发送频率限制
    - 异步发送
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import base64
import json
import logging
import time
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """通知消息"""
    title: str
    content: str
    level: str = "INFO"  # INFO / WARNING / CRITICAL / FATAL
    timestamp: int = 0
    extra: Dict[str, Any] = None

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = int(time.time() * 1000)
        if self.extra is None:
            self.extra = {}

    @property
    def level_emoji(self) -> str:
        emojis = {
            "INFO": "ℹ️",
            "WARNING": "⚠️",
            "CRITICAL": "🚨",
            "FATAL": "💀",
        }
        return emojis.get(self.level, "📢")

    def to_markdown(self) -> str:
        """转为 Markdown 格式"""
        lines = [
            f"## {self.level_emoji} {self.title}",
            "",
            self.content,
            "",
            f"> 时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.timestamp / 1000))}",
            f"> 级别: {self.level}",
        ]
        if self.extra:
            lines.append("")
            for k, v in self.extra.items():
                lines.append(f"> {k}: {v}")
        return "\n".join(lines)


class NotificationChannel(ABC):
    """通知渠道基类"""

    def __init__(self, name: str, enabled: bool = True, rate_limit: int = 20):
        self._name = name
        self._enabled = enabled
        self._rate_limit = rate_limit  # 每分钟最大发送数
        self._send_times: list = []
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = value

    async def send(self, message: Message) -> bool:
        """发送消息（带频率限制）"""
        if not self._enabled:
            return False

        if not self._check_rate_limit():
            logger.warning(f"Channel {self._name} rate limited")
            return False

        try:
            success = await self._do_send(message)
            if success:
                self._send_times.append(time.time())
            return success
        except Exception as e:
            logger.error(f"Channel {self._name} send failed: {e}")
            return False

    @abstractmethod
    async def _do_send(self, message: Message) -> bool:
        """实际发送逻辑"""
        ...

    def _check_rate_limit(self) -> bool:
        """检查频率限制"""
        now = time.time()
        # 清理 60 秒前的记录
        self._send_times = [t for t in self._send_times if now - t < 60]
        return len(self._send_times) < self._rate_limit

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取 HTTP session"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """关闭连接"""
        if self._session and not self._session.closed:
            await self._session.close()


class DingTalkChannel(NotificationChannel):
    """钉钉机器人通知

    Webhook 格式：
        POST https://oapi.dingtalk.com/robot/send?access_token=xxx
        Body: {"msgtype": "markdown", "markdown": {"title": "...", "text": "..."}}

    签名（加签模式）：
        timestamp + "\n" + secret → HMAC-SHA256 → Base64 → URL encode
    """

    def __init__(
        self,
        webhook_url: str,
        secret: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(name="dingtalk", **kwargs)
        self._webhook_url = webhook_url
        self._secret = secret

    def _sign_url(self) -> str:
        """生成带签名的 URL"""
        if not self._secret:
            return self._webhook_url

        timestamp = str(int(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{self._secret}"
        hmac_code = hmac.new(
            self._secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return f"{self._webhook_url}&timestamp={timestamp}&sign={sign}"

    async def _do_send(self, message: Message) -> bool:
        url = self._sign_url()
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": message.title,
                "text": message.to_markdown(),
            },
        }

        session = await self._get_session()
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            if data.get("errcode") == 0:
                return True
            logger.error(f"DingTalk error: {data}")
            return False


class FeishuChannel(NotificationChannel):
    """飞书机器人通知

    Webhook 格式：
        POST https://open.feishu.cn/open-apis/bot/v2/hook/xxx
        Body: {"msg_type": "interactive", "card": {...}}

    签名：
        timestamp + "\n" + secret → HMAC-SHA256 → Base64
    """

    def __init__(
        self,
        webhook_url: str,
        secret: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(name="feishu", **kwargs)
        self._webhook_url = webhook_url
        self._secret = secret

    def _gen_sign(self) -> Dict[str, str]:
        """生成签名参数"""
        if not self._secret:
            return {}

        timestamp = str(int(time.time()))
        string_to_sign = f"{timestamp}\n{self._secret}"
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        sign = base64.b64encode(hmac_code).decode("utf-8")
        return {"timestamp": timestamp, "sign": sign}

    async def _do_send(self, message: Message) -> bool:
        # 飞书卡片消息
        color_map = {
            "INFO": "blue",
            "WARNING": "orange",
            "CRITICAL": "red",
            "FATAL": "red",
        }
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": f"{message.level_emoji} {message.title}"},
                "template": color_map.get(message.level, "blue"),
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": message.content,
                },
                {
                    "tag": "note",
                    "elements": [
                        {
                            "tag": "plain_text",
                            "content": f"级别: {message.level} | {time.strftime('%Y-%m-%d %H:%M:%S')}",
                        }
                    ],
                },
            ],
        }

        payload = {"msg_type": "interactive", "card": card}
        payload.update(self._gen_sign())

        session = await self._get_session()
        async with session.post(self._webhook_url, json=payload) as resp:
            data = await resp.json()
            if data.get("code") == 0 or data.get("StatusCode") == 0:
                return True
            logger.error(f"Feishu error: {data}")
            return False


class TelegramChannel(NotificationChannel):
    """Telegram Bot 通知

    API:
        POST https://api.telegram.org/bot{token}/sendMessage
        Body: {"chat_id": "...", "text": "...", "parse_mode": "Markdown"}
    """

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        api_base: str = "https://api.telegram.org",
        **kwargs,
    ):
        super().__init__(name="telegram", **kwargs)
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._api_base = api_base

    async def _do_send(self, message: Message) -> bool:
        url = f"{self._api_base}/bot{self._bot_token}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": message.to_markdown(),
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }

        session = await self._get_session()
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            if data.get("ok"):
                return True
            logger.error(f"Telegram error: {data}")
            return False


class WebhookChannel(NotificationChannel):
    """通用 Webhook 通知

    发送 JSON POST 到指定 URL。
    """

    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        super().__init__(name="webhook", **kwargs)
        self._url = url
        self._headers = headers or {"Content-Type": "application/json"}

    async def _do_send(self, message: Message) -> bool:
        payload = {
            "title": message.title,
            "content": message.content,
            "level": message.level,
            "timestamp": message.timestamp,
            "markdown": message.to_markdown(),
            "extra": message.extra,
        }

        session = await self._get_session()
        async with session.post(self._url, json=payload, headers=self._headers) as resp:
            return 200 <= resp.status < 300
