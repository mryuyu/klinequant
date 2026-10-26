"""AppState.heal_http_client 共享连接池自愈单测

背景（v9.158）：代理断流期间共享 httpx.AsyncClient 池内 keep-alive 隧道全成死连接，
恢复后 trade/backtest 路由的请求仍即刻失败且永不自愈（client 只在 is_closed 时重建）。
heal_http_client 对传输层异常主动丢弃旧池，下次 get_http_client 重建。
"""
import asyncio

import httpx

from klinequant.gateway.state import AppState


def _run(coro):
    return asyncio.run(coro)


def test_heal_drops_client_on_transport_error():
    """传输层异常（httpx.TransportError 子类）→ 旧 client 被丢弃，下次 get 重建新实例"""
    async def scenario():
        st = AppState()
        c1 = st.get_http_client()
        await st.heal_http_client(httpx.ConnectError(""))
        assert st._http_client is None
        c2 = st.get_http_client()
        assert c2 is not c1
        assert c1.is_closed          # 旧池已关闭
        assert not c2.is_closed
        await c2.aclose()
    _run(scenario())


def test_heal_keeps_client_on_non_transport_error():
    """非传输层异常（业务/解析错误）→ client 原样保留，不触发重建"""
    async def scenario():
        st = AppState()
        c1 = st.get_http_client()
        await st.heal_http_client(ValueError("bad json"))
        assert st._http_client is c1
        assert not c1.is_closed
        await st.heal_http_client(httpx.HTTPStatusError(
            "401", request=httpx.Request("GET", "https://x/"),
            response=httpx.Response(401),
        ))
        assert st._http_client is c1  # HTTPStatusError 非 TransportError 子类
        await c1.aclose()
    _run(scenario())


def test_heal_noop_without_client():
    """client 尚未创建（None）时 heal 不抛异常，为幂等空操作"""
    async def scenario():
        st = AppState()
        await st.heal_http_client(httpx.ReadTimeout(""))   # 空 str 正是线上日志空白的形态
        assert st._http_client is None
    _run(scenario())


def test_heal_survives_aclose_failure():
    """旧 client aclose 再抛异常也不影响自愈：_http_client 已先置 None，下次 get 照常重建"""
    async def scenario():
        st = AppState()

        class _BrokenClient(httpx.AsyncClient):
            async def aclose(self):
                raise RuntimeError("close on dead pool")

        st._http_client = _BrokenClient()
        await st.heal_http_client(httpx.ConnectError(""))
        assert st._http_client is None
        c = st.get_http_client()
        assert isinstance(c, httpx.AsyncClient)
        await c.aclose()
    _run(scenario())
