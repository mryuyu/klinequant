"""同花顺 A 股市场源插件（ths_source）单元测试

覆盖：
    - 品种编码归一化（6 位简码 → THSCODE）
    - m1 桶标签与交易时段网格聚合（收盘时刻惯例/午休/盘后边界）
    - 周 K close-label（周五）→ open-label（当周周一）归一化（历史 fetch + 实时 _week_bar）
    - fetch_klines：标准 bar 转换/北京时间戳/翻页过滤/精度兜底
    - fetch_ticker：快照字段映射 + 缓存
    - list_symbols：股票 + 指数目录合并与兜底
    - stream 聚合：快照累计量差分 → m1/5m/1d 发布
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from klinequant.gateway.market_sources.manager import market_manager
from klinequant.gateway.market_sources.ths_source import (
    ThsApi,
    ThsSource,
    _bucket_label,
    _display_code,
    _is_conn_dead,
    _m1_label,
    _normalize_code,
    _week_open_ms,
)

_BJ = timezone(timedelta(hours=8))


def _bj_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class _FakeThsDriver:
    """驱动桩：记录调用，按预置数据返回（None=失败，[]=成功无数据）"""

    def __init__(self, klines: dict | None = None, snapshot: list | None = None,
                 stocks: list | None = None, indexes: list | None = None):
        self._klines = klines or {}
        self._snapshot = snapshot if snapshot is not None else []
        self._stocks = stocks
        self._indexes = indexes
        self.klines_calls: list[tuple] = []
        self.snapshot_calls: list[list] = []
        self.connected = True
        self.is_guest = False

    def connect(self) -> bool:
        return self.connected

    def klines(self, code, interval, count=None, start=None, end=None):
        self.klines_calls.append((code, interval, count, start, end))
        return self._klines.get(interval)

    def snapshot(self, codes):
        self.snapshot_calls.append(list(codes))
        return self._snapshot

    def stock_list(self):
        return self._stocks

    def index_list(self):
        return self._indexes


def _kline_row(dt, o, h, lo, c, v) -> dict:
    return {
        "时间": dt, "开盘价": o, "最高价": h, "最低价": lo,
        "收盘价": c, "成交量": v, "总金额": 0,
    }


# ─── 展示码：THSCODE → 用户熟知的 6 位纯数字交易所码（路由码不受影响） ───

def test_display_code():
    assert _display_code("USHA600519") == "600519"       # 股票取末 6 位
    assert _display_code("USZA000001") == "000001"       # 平安银行（与上证指数撞码，列表双行区分）
    assert _display_code("USZI399001") == "399001"       # 深指数取末 6 位
    assert _display_code("USHI1A0001") == "000001"       # 上证指数（1A 段静态表，实测）
    assert _display_code("USHI1B0016") == "000016"       # 上证50（1B 段规则，实测）
    assert _display_code("USHI1B0688") == "000688"       # 科创50
    assert _display_code("USHI1C0003") == "000001"       # 上证指数重复条目（静态表同 1A0001）
    assert _display_code("USHI1B0001") == "000004"       # 工业指数静态表（防与上证指数撞码）
    assert _display_code("USHI1A0002") == "000002"       # 上证Ａ股指数（实测）
    assert _display_code("USHI1C0002") == "1C0002"       # 领先指标无交易所码，兜底原码段


# ─── 品种编码归一化 ───

def test_source_meta_region():
    src = ThsSource(driver=_FakeThsDriver())
    assert src.meta()["region"] == "cn"                        # 国内源驱动前端「国内」分段


def test_normalize_code():
    assert _normalize_code("USHA600519") == "USHA600519"
    assert _normalize_code("usha600519") == "USHA600519"
    assert _normalize_code("USHI1A0001") == "USHI1A0001"      # 指数代码段含字母
    assert _normalize_code("600519") == "USHA600519"      # 6 开头 → 沪市
    assert _normalize_code("300750") == "USZA300750"      # 3 开头 → 深市
    assert _normalize_code("000858") == "USZA000858"
    with pytest.raises(ValueError):
        _normalize_code("BTCUSDT")
    with pytest.raises(ValueError):
        _normalize_code("400001")                          # 北交所简码不支持两义推断


# ─── m1 桶标签与 tf 网格（收盘时刻惯例，对齐 thsdk 历史 1m 标签） ───

def test_m1_label_session_rules():
    assert _m1_label(9 * 3600 + 1800) == 9 * 3600 + 1860          # 09:30:00 开盘 → 09:31
    assert _m1_label(9 * 3600 + 1799) == 9 * 3600 + 1860          # 集合竞价归首桶
    assert _m1_label(9 * 3600 + 1861) == 9 * 3600 + 1920          # 09:31:01 → 09:32
    assert _m1_label(11 * 3600 + 1800) == 11 * 3600 + 1800        # 11:30:00 边界钳制
    assert _m1_label(12 * 3600) == 11 * 3600 + 1800               # 午休 → 上午尾桶
    assert _m1_label(13 * 3600 + 5) == 13 * 3600 + 60             # 13:00:05 → 13:01
    assert _m1_label(15 * 3600) == 15 * 3600                      # 15:00:00 收盘钳制
    assert _m1_label(15 * 3600 + 60) == 15 * 3600                 # 盘后 → 收盘桶


def test_bucket_label_session_grid():
    assert _bucket_label(9 * 3600 + 1860, 300) == 9 * 3600 + 2100   # 09:31 → 5m 桶 09:35
    assert _bucket_label(9 * 3600 + 2100, 300) == 9 * 3600 + 2100   # 09:35 落本桶
    assert _bucket_label(11 * 3600 + 1800, 1800) == 11 * 3600 + 1800  # 11:30 → 30m 桶 11:30
    assert _bucket_label(13 * 3600 + 60, 3600) == 14 * 3600         # 13:01 → 1h 桶 14:00
    assert _bucket_label(10 * 3600 + 2700, 3600) == 11 * 3600 + 1800  # 10:45 → 1h 桶 11:30


# ─── 周 K close-label（周五）→ open-label（当周周一）归一化 ───

def test_week_open_ms_relabel_to_monday():
    """thsdk 原生周 K 按当周最后交易日标记，统一 relabel 到日历周一（北京），
    与派生月/季/年线及币安/MT5 周线口径一致，修复跨周期画线锚点错位一根。"""
    mon = lambda d: _bj_ms(datetime(*d, tzinfo=_BJ))            # noqa: E731
    # 常规周五（2026-06-26）→ 当周周一（06-22）
    assert _week_open_ms(mon((2026, 6, 26))) == mon((2026, 6, 22))
    # 假日短周：最后交易日为周四（2026-09-10）→ 当周周一（09-07）
    assert _week_open_ms(mon((2026, 9, 10))) == mon((2026, 9, 7))
    # 已是周一（2026-08-31）→ 原样不变（幂等）
    assert _week_open_ms(mon((2026, 8, 31))) == mon((2026, 8, 31))


# ─── fetch_klines：标准 bar 转换 / 北京时间戳 / 翻页 / 精度 ───

async def test_fetch_klines_conversion_and_precision():
    day = datetime(2026, 8, 28)                      # 日 K naive（北京时间）
    m1 = datetime(2026, 8, 28, 15, 0, tzinfo=_BJ)    # 分钟线带时区
    driver = _FakeThsDriver(klines={
        "day": [_kline_row(day, 1289, 1297.89, 1288, 1297.4, 1612611)],
        "1m": [_kline_row(m1, 1297.4, 1297.4, 1297.4, 1297.4, 28300)],
    })
    src = ThsSource(driver=driver)
    bars = await src.fetch_klines("600519", "1d", limit=10)
    assert bars[0]["timestamp"] == _bj_ms(day.replace(tzinfo=_BJ))
    assert bars[0]["close"] == 1297.4 and bars[0]["volume"] == 1612611
    bars1m = await src.fetch_klines("USHA600519", "1m", limit=10)
    assert bars1m[0]["timestamp"] == _bj_ms(m1)
    assert src.price_precision("USHA600519") == 2    # A 股精度下限 2 位
    with pytest.raises(ValueError):
        await src.fetch_klines("600519", "4h", limit=10)


async def test_fetch_klines_end_time_paging():
    d1 = datetime(2026, 8, 26)
    d2 = datetime(2026, 8, 27)
    d3 = datetime(2026, 8, 28)
    driver = _FakeThsDriver(klines={"day": [
        _kline_row(d1, 1, 1, 1, 1, 1), _kline_row(d2, 2, 2, 2, 2, 2), _kline_row(d3, 3, 3, 3, 3, 3),
    ]})
    src = ThsSource(driver=driver)
    end = _bj_ms(d2.replace(tzinfo=_BJ))
    bars = await src.fetch_klines("600519", "1d", limit=10, end_time=end)
    assert [b["close"] for b in bars] == [1.0, 2.0]          # end_time（含）之前的页
    code, interval, count, start, end_arg = driver.klines_calls[-1]
    assert count is None and start is not None and end_arg is not None   # 翻页走区间查询


async def test_fetch_klines_weekly_relabel_to_monday():
    """历史周 K：thsdk 原生周五标记 → relabel 当周周一；OHLCV 与 bar 顺序不变"""
    fri1 = datetime(2026, 6, 26)     # 周五
    fri2 = datetime(2026, 7, 3)      # 周五
    driver = _FakeThsDriver(klines={"week": [
        _kline_row(fri1, 1.99, 2.04, 1.93, 2.02, 111),
        _kline_row(fri2, 2.02, 2.05, 1.95, 1.96, 222),
    ]})
    src = ThsSource(driver=driver)
    bars = await src.fetch_klines("600545", "1w", limit=10)
    assert [b["timestamp"] for b in bars] == [
        _bj_ms(datetime(2026, 6, 22, tzinfo=_BJ)),   # 当周周一
        _bj_ms(datetime(2026, 6, 29, tzinfo=_BJ)),   # 当周周一
    ]
    assert [b["close"] for b in bars] == [2.02, 1.96]   # OHLCV 不受 relabel 影响


async def test_fetch_klines_deep_paging_across_chunk_cap(monkeypatch):
    """超单次拉取上限的深分页：区间分页拼接拿全更早历史（上市至今覆盖）"""
    import klinequant.gateway.market_sources.ths_source as ths_mod
    monkeypatch.setattr(ths_mod, "_MAX_CHUNK", 2)   # 模拟 thsdk 单次上限截断
    days = [datetime(2026, 8, d) for d in range(24, 29)]   # 5 根连续日 K
    rows = [_kline_row(d, i + 1, i + 1, i + 1, i + 1, i + 1) for i, d in enumerate(days)]

    class _WindowedDriver(_FakeThsDriver):
        """区间查询按 [start, end] 过滤 + 单次上限截断（仿 thsdk 实测行为）"""
        def klines(self, code, interval, count=None, start=None, end=None):
            self.klines_calls.append((code, interval, count, start, end))
            if count is not None:
                return rows[-count:]
            hit = [r for r in rows if start <= r["时间"].replace(tzinfo=_BJ) <= end]
            return hit[-ths_mod._MAX_CHUNK:]

    src = ThsSource(driver=_WindowedDriver())
    bars = await src.fetch_klines(
        "600519", "1d", limit=5, end_time=_bj_ms(days[-1].replace(tzinfo=_BJ))
    )
    assert [b["close"] for b in bars] == [1.0, 2.0, 3.0, 4.0, 5.0]   # 5 根全量拿齐


# ─── fetch_ticker：快照字段映射 + 缓存 ───

async def test_fetch_ticker_from_snapshot():
    driver = _FakeThsDriver(snapshot=[{
        "代码": "USHA600519", "名称": "贵州茅台", "价格": 1297.4, "昨收价": 1292.3,
        "开盘价": 1289, "最高价": 1297.89, "最低价": 1288, "成交量": 1612611,
    }])
    src = ThsSource(driver=driver)
    t = await src.fetch_ticker("USHA600519")
    assert t["last_price"] == 1297.4
    assert t["price_change_pct"] == pytest.approx((1297.4 / 1292.3 - 1) * 100)
    assert t["high_24h"] == 1297.89 and t["volume_24h"] == 1612611
    await src.fetch_ticker("USHA600519")                       # 缓存命中，不再调驱动
    assert len(driver.snapshot_calls) == 1


# ─── list_symbols：股票 + 指数目录合并 / 失败兜底默认集 ───

async def test_list_symbols_merge_and_fallback():
    driver = _FakeThsDriver(
        stocks=[{"代码": "USHA600519", "名称": "贵州茅台"}],
        indexes=[{"代码": "USHI1A0001", "名称": "上证指数"}],
    )
    rows = await ThsSource(driver=driver).list_symbols()
    assert {"symbol": "USHA600519", "name": "贵州茅台", "type": "stock", "code": "600519"} in rows
    assert {"symbol": "USHI1A0001", "name": "上证指数", "type": "index", "code": "000001"} in rows

    driver_fail = _FakeThsDriver(stocks=None, indexes=None)
    rows = await ThsSource(driver=driver_fail).list_symbols()
    assert rows and rows[0]["symbol"] == "USHI1A0001"          # 兜底默认品种集（含展示码）
    assert rows[0]["code"] == "000001"


# ─── 盘中聚合：快照累计量差分 → m1/5m/1d 发布 ───

async def test_update_symbol_aggregation(monkeypatch):
    driver = _FakeThsDriver(klines={"1m": []})                 # 无种子，冷启动
    src = ThsSource(driver=driver)
    published: list[tuple] = []

    async def fake_publish(exchange, symbol, tf, bar):
        published.append((symbol, tf, bar))
        return True

    monkeypatch.setattr(market_manager, "publish_bar", fake_publish)
    src._sym_tfs = {"USHA600519": {"1m", "5m", "1d"}}
    today = date(2026, 8, 31)
    base = datetime(2026, 8, 31, tzinfo=_BJ)

    def snap(price, cum, hh, mm, ss):
        return ({"价格": price, "成交量": cum, "开盘价": 10.0, "最高价": price, "最低价": price},
                base.replace(hour=hh, minute=mm, second=ss))

    # 首帧：只建累计量基线，m1 桶差分量为 0
    row, now = snap(10.5, 1000, 9, 30, 20)
    await src._update_symbol("USHA600519", row, today, now)
    m1 = [b for s, tf, b in published if tf == "1m"][-1]
    assert m1["timestamp"] == _bj_ms(base.replace(hour=9, minute=31))
    assert m1["volume"] == 0.0 and m1["close"] == 10.5
    d1 = [b for s, tf, b in published if tf == "1d"][-1]
    assert d1["volume"] == 1000.0                              # 日 K 用快照当日累计

    # 同桶续涨：差分 500 手计入当前桶，5m 聚合桶同步
    row, now = snap(10.7, 1500, 9, 30, 50)
    await src._update_symbol("USHA600519", row, today, now)
    m1 = [b for s, tf, b in published if tf == "1m"][-1]
    assert m1["volume"] == 500.0 and m1["high"] == 10.7
    m5 = [b for s, tf, b in published if tf == "5m"][-1]
    assert m5["timestamp"] == _bj_ms(base.replace(hour=9, minute=35))
    assert m5["volume"] == 500.0 and m5["close"] == 10.7

    # 跨分钟：新桶开桶，差分 100 手
    row, now = snap(10.4, 1600, 9, 31, 10)
    await src._update_symbol("USHA600519", row, today, now)
    m1 = [b for s, tf, b in published if tf == "1m"][-1]
    assert m1["timestamp"] == _bj_ms(base.replace(hour=9, minute=32))
    assert m1["volume"] == 100.0 and m1["open"] == 10.4


# ─── 周 K：种子 + 当日增量 ───

async def test_week_bar_seed_and_delta(monkeypatch):
    today = date(2026, 8, 31)                                  # 周一
    week_ts = datetime(2026, 8, 31)                            # 种子落本周
    driver = _FakeThsDriver(klines={
        "1m": [],
        "week": [_kline_row(week_ts, 9.5, 10.8, 9.4, 10.2, 50000)],
        "day": [_kline_row(datetime(2026, 8, 31), 10.0, 10.5, 9.9, 10.3, 800)],
    })
    src = ThsSource(driver=driver)
    published: list[tuple] = []

    async def fake_publish(exchange, symbol, tf, bar):
        published.append((symbol, tf, bar))
        return True

    monkeypatch.setattr(market_manager, "publish_bar", fake_publish)
    src._sym_tfs = {"USZA300750": {"1w"}}
    base = datetime(2026, 8, 31, tzinfo=_BJ)
    row = {"价格": 10.6, "成交量": 1000, "开盘价": 10.0, "最高价": 10.7, "最低价": 9.9}
    await src._update_symbol("USZA300750", row, today, base.replace(hour=10))
    wk = [b for s, tf, b in published if tf == "1w"][-1]
    assert wk["open"] == 9.5                                   # 周开盘保持种子值
    assert wk["high"] == 10.8 and wk["close"] == 10.6
    assert wk["volume"] == 50000 + (1000 - 800)                # 种子量 + 当日增量（扣除基线）
    assert wk["timestamp"] == _bj_ms(datetime(2026, 8, 31, tzinfo=_BJ))   # 周一种子 → 周一标签


async def test_week_bar_relabel_friday_seed_to_monday(monkeypatch):
    """实时周 K：thsdk 周五种子（close-label）→ 发布 timestamp relabel 到当周周一，
    与历史周线口径一致（否则实时 bar 与历史 bar 标签错位，前端无法合并）。"""
    today = date(2026, 9, 4)                                   # 周五
    driver = _FakeThsDriver(klines={
        "1m": [],
        "week": [_kline_row(datetime(2026, 9, 4), 9.5, 10.8, 9.4, 10.2, 50000)],   # 周五种子
        "day": [_kline_row(datetime(2026, 9, 4), 10.0, 10.5, 9.9, 10.3, 800)],
    })
    src = ThsSource(driver=driver)
    published: list[tuple] = []

    async def fake_publish(exchange, symbol, tf, bar):
        published.append((symbol, tf, bar))
        return True

    monkeypatch.setattr(market_manager, "publish_bar", fake_publish)
    src._sym_tfs = {"USZA300750": {"1w"}}
    base = datetime(2026, 9, 4, tzinfo=_BJ)
    row = {"价格": 10.6, "成交量": 1000, "开盘价": 10.0, "最高价": 10.7, "最低价": 9.9}
    await src._update_symbol("USZA300750", row, today, base.replace(hour=10))
    wk = [b for s, tf, b in published if tf == "1w"][-1]
    assert wk["timestamp"] == _bj_ms(datetime(2026, 8, 31, tzinfo=_BJ))   # 周五 9/4 → 当周周一 8/31
    assert wk["open"] == 9.5 and wk["close"] == 10.6


# ─── 断连自愈：传输层连接死亡必须丢弃连接触发重连 ───

_WSASEND_ERR = (
    "[thsdk]QueryData 错误:write tcp 192.168.1.254:1828->119.3.43.91:9602: "
    "wsasend: An established connection was aborted by the software in your host machine."
)


def test_is_conn_dead_matches_transport_errors_only():
    assert _is_conn_dead(_WSASEND_ERR)                         # 实测会话中止错误串
    assert _is_conn_dead("read tcp 1.2.3.4:9602: reset by peer")
    assert _is_conn_dead("i/o timeout")
    assert _is_conn_dead("unexpected EOF")
    # 业务级错误不得误判为断连（否则无谓重连风暴）
    assert not _is_conn_dead("请求太快")
    assert not _is_conn_dead("限频")
    assert not _is_conn_dead("无数据")
    assert not _is_conn_dead("代码非法")


def test_call_drops_dead_connection_on_transport_error():
    """连接被中止的错误必须丢弃连接（connected=False/_ths=None），
    令下次调用经冷却重连；否则死 socket 被永久复用，K 线久不自愈（只能重启进程）。"""

    class _Res:
        success = False
        error = _WSASEND_ERR
        data = None

    class _FakeThs:
        def klines(self, *a, **k):
            return _Res()

    api = ThsApi()
    api.connected = True
    api._ths = _FakeThs()
    assert api._call(lambda t: t.klines("USHA600519")) is None
    assert api.connected is False and api._ths is None          # 死连接已丢弃


def test_call_keeps_connection_on_business_error():
    """业务级失败（非断连）不得误丢连接，避免无谓重登录"""

    class _Res:
        success = False
        error = "请求太快，请稍后再试"
        data = None

    class _FakeThs:
        def klines(self, *a, **k):
            return _Res()

    api = ThsApi()
    api.connected = True
    ths = _FakeThs()
    api._ths = ths
    assert api._call(lambda t: t.klines("USHA600519"), retries=0) is None
    assert api.connected is True and api._ths is ths            # 连接保留
