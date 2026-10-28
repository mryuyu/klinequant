"""图表模版路由单测（gateway/routers/template.py）

覆盖：
  - 存储层：_read 缺失/损坏回退空、_write 原子落盘 + 中文保真、读改写往返
  - 端点：GET 空态/有数据、PUT 整体覆盖、非法载荷优雅报错

用 monkeypatch 把 TPL_FILE 指向 tmp_path，隔离真实 data/state 存档。
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from klinequant.gateway.routers import template


@pytest.fixture
def tpl_file(tmp_path, monkeypatch):
    """把模版文件重定向到临时目录，避免污染真实 data/state。"""
    f = tmp_path / "chart_templates.json"
    monkeypatch.setattr(template, "TPL_FILE", f)
    return f


@pytest.fixture
def client():
    """最小 app 只挂 template 路由（避开 create_app 的数据源 bootstrap）。"""
    app = FastAPI()
    app.include_router(template.router)
    return TestClient(app)


SAMPLE = [{
    "name": "模板A", "ts": 123,
    "data": {"indLayout": [{"type": "ma", "params": {"short": 7, "long": 25}}],
             "colors": {"up": "#0ecb81", "down": "#f6465d"}},
}]


# ─── 存储层 ───


def test_read_missing_file_returns_empty(tpl_file):
    assert not tpl_file.exists()
    assert template._read() == []


def test_read_corrupt_file_returns_empty(tpl_file):
    tpl_file.write_text("{not valid json", encoding="utf-8")
    assert template._read() == []


def test_write_then_read_roundtrip_preserves_chinese(tpl_file):
    assert template._write(SAMPLE) is True
    assert tpl_file.is_file()
    assert template._read() == SAMPLE
    # 中文以原文落盘（ensure_ascii=False），非 \uXXXX 转义
    assert "模板A" in tpl_file.read_text(encoding="utf-8")


def test_write_leaves_no_tmp_file(tpl_file):
    template._write(SAMPLE)
    assert not list(tpl_file.parent.glob("*.tmp"))


# ─── 端点 ───


def test_get_empty(client, tpl_file):
    assert client.get("/api/chart-templates").json() == {"templates": []}


def test_put_then_get_roundtrip(client, tpl_file):
    r = client.put("/api/chart-templates", json={"templates": SAMPLE})
    assert r.json() == {"ok": True, "count": 1}
    assert client.get("/api/chart-templates").json() == {"templates": SAMPLE}


def test_put_roundtrips_drawing_heavy_template(client, tpl_file):
    """v9.169：模版 data 携带画线/样式偏好/fib 级别（前端全量打包），
    后端透明往返不丢字段、不拒大载荷。"""
    tpl = [{
        "name": "带画线模板", "ts": 999,
        "data": {
            "indLayout": [{"type": "ma", "params": {"short": 7}}],
            "colors": {"up": "#0ecb81", "down": "#f6465d"},
            "drawings": {
                "binance:BTCUSDT": [
                    {"id": "abc", "type": "trend", "tfAll": True,
                     "style": {"color": "#2962ff", "lineWidth": 2, "lineStyle": 0},
                     "anchors": [{"time": 1700000000, "price": 60000.5},
                                 {"time": 1700003600, "price": 61000.0}]},
                ],
                "binance:ETHUSDT": [],
            },
            "drawPref": {"defaultStyle": {"color": "#2962ff", "lineWidth": 1, "lineStyle": 0},
                         "magnetOn": True, "continuousDraw": False,
                         "toolStyles": {"fib": {"lineWidth": 1}}},
            "fibLevels": [{"r": 0.0, "c": "#787b86"}, {"r": 0.5, "c": "#f44337"}],
        },
    }]
    r = client.put("/api/chart-templates", json={"templates": tpl})
    assert r.json() == {"ok": True, "count": 1}
    got = client.get("/api/chart-templates").json()["templates"]
    assert got == tpl   # 画线/样式/fib 字段原样往返，后端不解析 data 内部结构
    assert got[0]["data"]["drawings"]["binance:BTCUSDT"][0]["style"]["lineWidth"] == 2


def test_put_overwrites_whole_list(client, tpl_file):
    client.put("/api/chart-templates", json={"templates": SAMPLE})
    client.put("/api/chart-templates", json={"templates": []})
    assert client.get("/api/chart-templates").json() == {"templates": []}


def test_put_invalid_payload_reports_error(client, tpl_file):
    r = client.put("/api/chart-templates", json={"nope": 1})
    body = r.json()
    assert body["ok"] is False
    assert "error" in body
