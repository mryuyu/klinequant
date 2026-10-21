"""api.web() + webstack 后台单例 单测。

覆盖态③（策略 + GUI）开关：
  - webstack.ensure_background 幂等拉起（进程级单例，多实例共享）
  - webstack.stop_background 优雅关停（should_exit + join + 清单例）
  - KqApi.web(run=True/False) 正确委托（web() 不使用 self，用 mock-self 调未绑定方法）
全部 mock，不起真实 uvicorn / 不占端口。
"""
from __future__ import annotations

import threading
from unittest import mock

import pytest

from klinequant import webstack
from klinequant.kqsdk.api import KqApi


@pytest.fixture(autouse=True)
def _reset_bg_singleton():
    """每例前后清空后台单例，避免测试间串扰。"""
    webstack._bg_server = None
    webstack._bg_thread = None
    yield
    webstack._bg_server = None
    webstack._bg_thread = None


def _fake_thread(alive: bool = True) -> mock.MagicMock:
    t = mock.MagicMock(spec=threading.Thread)
    t.is_alive.return_value = alive
    return t


def test_ensure_background_starts_once_idempotent():
    """首次拉起调用 start_in_background；再调直接复用，不重复起（避免端口冲突）。"""
    fake_server = mock.MagicMock()
    fake_thread = _fake_thread(alive=True)
    with mock.patch.object(
        webstack, "start_in_background", return_value=(fake_server, fake_thread)
    ) as m:
        s1, t1 = webstack.ensure_background()
        s2, t2 = webstack.ensure_background()
    assert m.call_count == 1
    assert (s1, t1) == (fake_server, fake_thread)
    assert (s2, t2) == (fake_server, fake_thread)
    assert webstack.is_background_running() is True


def test_is_background_running_false_when_thread_dead():
    webstack._bg_thread = _fake_thread(alive=False)
    assert webstack.is_background_running() is False


def test_stop_background_sets_should_exit_and_joins():
    fake_server = mock.MagicMock()
    fake_thread = _fake_thread(alive=True)
    webstack._bg_server = fake_server
    webstack._bg_thread = fake_thread
    webstack.stop_background(timeout=1.0)
    assert fake_server.should_exit is True
    fake_thread.join.assert_called_once_with(timeout=1.0)
    assert webstack._bg_server is None
    assert webstack._bg_thread is None


def test_stop_background_noop_when_not_running():
    """未运行时 stop 不报错、不清空。"""
    webstack.stop_background()
    assert webstack._bg_server is None


def test_api_web_run_true_delegates_to_ensure():
    """api.web(run=True) → webstack.ensure_background（透传 host/port/open_path）。"""
    fake_self = mock.MagicMock()
    with mock.patch.object(webstack, "ensure_background") as ensure:
        KqApi.web(fake_self, run=True, host="0.0.0.0", port=9000, open_path="/")
    ensure.assert_called_once_with("0.0.0.0", 9000, open_browser=True, open_path="/")


def test_api_web_run_false_delegates_to_stop():
    """api.web(run=False) → webstack.stop_background（态② headless 关停）。"""
    fake_self = mock.MagicMock()
    with mock.patch.object(webstack, "stop_background") as stop:
        KqApi.web(fake_self, run=False)
    stop.assert_called_once_with()


def test_api_web_default_run_is_true():
    """api.web() 无参默认 run=True（拉起前端）。"""
    fake_self = mock.MagicMock()
    with mock.patch.object(webstack, "ensure_background") as ensure:
        KqApi.web(fake_self)
    ensure.assert_called_once()
