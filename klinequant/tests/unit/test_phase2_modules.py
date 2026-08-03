"""Phase 2 新模块单元测试

覆盖：
    - 策略热加载器 (StrategyHotLoader)
    - 参数优化器 (ParameterOptimizer)
    - 告警通知 (AlertManager + Channels)
"""
from __future__ import annotations

import tempfile
import time
from decimal import Decimal
from pathlib import Path
from typing import Optional

import polars as pl
import pytest

from core.strategy_engine.hot_loader import (
    HotLoadEvent,
    LoadStatus,
    StrategyHotLoader,
    StrategyModuleInfo,
)
from core.strategy_engine.manager import StrategyManager, StrategyStatus
from core.strategy_engine.context import StrategyInfo
from core.backtest_engine.optimizer import (
    OptimizationConfig,
    OptimizationReport,
    OptimizationResult,
    ParameterOptimizer,
    ParamRange,
)
from core.notification.alert_manager import AlertLevel, AlertManager, AlertRule, AlertEvent
from core.notification.channels import Message, DingTalkChannel, FeishuChannel, TelegramChannel, WebhookChannel


# ─── 辅助 ───


def make_kline_df(n: int = 200) -> pl.DataFrame:
    """生成测试 K 线"""
    import random
    random.seed(42)
    prices = [50000.0]
    for i in range(1, n):
        prices.append(prices[-1] * (1 + random.uniform(-0.01, 0.01)))

    return pl.DataFrame({
        "timestamp": [1000000 + i * 60000 for i in range(n)],
        "open": prices,
        "high": [p * 1.002 for p in prices],
        "low": [p * 0.998 for p in prices],
        "close": [p * 1.001 for p in prices],
        "volume": [100.0] * n,
    })


SAMPLE_STRATEGY = '''
"""测试策略"""
from typing import Optional
import polars as pl
from core.strategy_engine.base import StrategyBase

class SampleStrategy(StrategyBase):
    def on_init(self) -> None:
        self.ctx.set_param("period", 14)

    def on_bar(self, df: pl.DataFrame, bar_index: int) -> Optional[str]:
        if bar_index < 14:
            return None
        if bar_index == 50:
            return "LONG"
        if bar_index == 100:
            return "CLOSE"
        return None

    def on_stop(self) -> None:
        pass
'''

SAMPLE_STRATEGY_V2 = '''
"""测试策略 v2"""
from typing import Optional
import polars as pl
from core.strategy_engine.base import StrategyBase

class SampleStrategy(StrategyBase):
    def on_init(self) -> None:
        self.ctx.set_param("period", 20)

    def on_bar(self, df: pl.DataFrame, bar_index: int) -> Optional[str]:
        if bar_index < 20:
            return None
        if bar_index == 60:
            return "SHORT"
        if bar_index == 120:
            return "CLOSE"
        return None

    def on_stop(self) -> None:
        pass
'''


# ─── 策略热加载测试 ───


class TestStrategyHotLoader:
    def test_load_file(self, tmp_path):
        """加载策略文件"""
        # 写入策略文件
        strategy_file = tmp_path / "sample.py"
        strategy_file.write_text(SAMPLE_STRATEGY, encoding="utf-8")

        manager = StrategyManager()
        loader = StrategyHotLoader(manager, strategy_dir=str(tmp_path))

        status = loader.load_file(strategy_file)
        assert status == LoadStatus.SUCCESS
        assert "sample" in loader.loaded_modules
        assert "sample" in manager.get_registered()

    def test_load_all(self, tmp_path):
        """加载目录下所有策略"""
        (tmp_path / "strat_a.py").write_text(SAMPLE_STRATEGY, encoding="utf-8")
        (tmp_path / "_private.py").write_text("# private", encoding="utf-8")

        manager = StrategyManager()
        loader = StrategyHotLoader(manager, strategy_dir=str(tmp_path))

        results = loader.load_all()
        assert "strat_a.py" in results
        assert "_private.py" not in results  # 下划线开头跳过
        assert results["strat_a.py"] == LoadStatus.SUCCESS

    def test_reload_strategy(self, tmp_path):
        """热重载策略"""
        strategy_file = tmp_path / "sample.py"
        strategy_file.write_text(SAMPLE_STRATEGY, encoding="utf-8")

        manager = StrategyManager()
        loader = StrategyHotLoader(manager, strategy_dir=str(tmp_path))
        loader.load_file(strategy_file)

        # 修改文件
        strategy_file.write_text(SAMPLE_STRATEGY_V2, encoding="utf-8")

        status = loader.reload_strategy("sample")
        assert status == LoadStatus.SUCCESS

        info = loader.loaded_modules["sample"]
        assert info.version == 2

    def test_rollback(self, tmp_path):
        """版本回滚"""
        strategy_file = tmp_path / "sample.py"
        strategy_file.write_text(SAMPLE_STRATEGY, encoding="utf-8")

        manager = StrategyManager()
        loader = StrategyHotLoader(manager, strategy_dir=str(tmp_path))
        loader.load_file(strategy_file)

        # 更新到 v2
        strategy_file.write_text(SAMPLE_STRATEGY_V2, encoding="utf-8")
        loader.reload_strategy("sample")
        assert loader.loaded_modules["sample"].version == 2

        # 回滚到 v1
        status = loader.rollback("sample")
        assert status == LoadStatus.SUCCESS
        assert loader.loaded_modules["sample"].version == 1

    def test_unload(self, tmp_path):
        """卸载策略"""
        strategy_file = tmp_path / "sample.py"
        strategy_file.write_text(SAMPLE_STRATEGY, encoding="utf-8")

        manager = StrategyManager()
        loader = StrategyHotLoader(manager, strategy_dir=str(tmp_path))
        loader.load_file(strategy_file)

        assert loader.unload_file("sample") is True
        assert "sample" not in loader.loaded_modules
        assert loader.unload_file("nonexist") is False

    def test_event_callback(self, tmp_path):
        """事件回调"""
        events = []
        strategy_file = tmp_path / "sample.py"
        strategy_file.write_text(SAMPLE_STRATEGY, encoding="utf-8")

        manager = StrategyManager()
        loader = StrategyHotLoader(manager, strategy_dir=str(tmp_path))
        loader.on_event(lambda e: events.append(e))

        loader.load_file(strategy_file)
        assert len(events) == 1
        assert events[0].event_type == "loaded"
        assert events[0].strategy_name == "sample"

    def test_invalid_file(self, tmp_path):
        """加载无效文件"""
        bad_file = tmp_path / "bad.py"
        bad_file.write_text("x = 1\n# no strategy class", encoding="utf-8")

        manager = StrategyManager()
        loader = StrategyHotLoader(manager, strategy_dir=str(tmp_path))

        status = loader.load_file(bad_file)
        assert status == LoadStatus.FAILED

    def test_history(self, tmp_path):
        """加载历史"""
        strategy_file = tmp_path / "sample.py"
        strategy_file.write_text(SAMPLE_STRATEGY, encoding="utf-8")

        manager = StrategyManager()
        loader = StrategyHotLoader(manager, strategy_dir=str(tmp_path))
        loader.load_file(strategy_file)

        strategy_file.write_text(SAMPLE_STRATEGY_V2, encoding="utf-8")
        loader.reload_strategy("sample")

        history = loader.get_history("sample")
        assert len(history) == 1  # v1 在历史中
        assert history[0]["version"] == 1

    def test_file_change_detection(self, tmp_path):
        """文件变更检测"""
        strategy_file = tmp_path / "sample.py"
        strategy_file.write_text(SAMPLE_STRATEGY, encoding="utf-8")

        manager = StrategyManager()
        loader = StrategyHotLoader(manager, strategy_dir=str(tmp_path))
        loader.load_file(strategy_file)

        # 未修改 → 无变化
        loader._check_changes()
        assert loader.loaded_modules["sample"].version == 1

        # 修改文件 → 自动重载
        strategy_file.write_text(SAMPLE_STRATEGY_V2, encoding="utf-8")
        loader._check_changes()
        assert loader.loaded_modules["sample"].version == 2


# ─── 参数优化器测试 ───


class TestParameterOptimizer:
    def test_param_range_linspace(self):
        """ParamRange.linspace"""
        pr = ParamRange.linspace("x", 0.0, 1.0, 5)
        assert pr.size == 5
        assert pr.values[0] == 0.0
        assert pr.values[-1] == 1.0

    def test_param_range_int(self):
        """ParamRange.range_int"""
        pr = ParamRange.range_int("p", 5, 20, 5)
        assert pr.values == [5, 10, 15, 20]
        assert pr.size == 4

    def test_param_range_choices(self):
        """ParamRange.choices"""
        pr = ParamRange.choices("mode", ["fast", "slow", "balanced"])
        assert pr.size == 3

    def test_grid_search_combinations(self):
        """网格搜索组合生成"""
        config = OptimizationConfig(method="grid")
        optimizer = ParameterOptimizer(config)

        ranges = [
            ParamRange("a", [1, 2, 3]),
            ParamRange("b", [10, 20]),
        ]
        combos = optimizer._generate_combinations(ranges)
        assert len(combos) == 6  # 3 × 2
        assert {"a": 1, "b": 10} in combos
        assert {"a": 3, "b": 20} in combos

    def test_random_search_combinations(self):
        """随机搜索组合生成"""
        config = OptimizationConfig(method="random", n_samples=5)
        optimizer = ParameterOptimizer(config)

        ranges = [
            ParamRange.range_int("x", 1, 100),
            ParamRange.range_int("y", 1, 100),
        ]
        combos = optimizer._generate_combinations(ranges)
        assert len(combos) == 5
        # 验证去重
        keys = [tuple(sorted(c.items())) for c in combos]
        assert len(keys) == len(set(keys))

    def test_optimize_basic(self):
        """基本优化流程"""
        config = OptimizationConfig(
            method="grid",
            max_workers=1,
            sort_by="total_return",
            top_n=3,
        )
        optimizer = ParameterOptimizer(config)
        data = make_kline_df(200)

        def strategy_fn(df: pl.DataFrame, bar_idx: int, params: dict) -> Optional[str]:
            trigger = params.get("trigger", 50)
            if bar_idx == trigger:
                return "LONG"
            if bar_idx == trigger + 50:
                return "CLOSE"
            return None

        ranges = [ParamRange("trigger", [30, 50, 70])]
        report = optimizer.optimize(data, strategy_fn, ranges)

        assert report.completed == 3
        assert report.failed == 0
        assert len(report.results) == 3
        assert report.best_params != {}
        assert report.duration_ms >= 0

    def test_optimize_with_indicator(self):
        """带指标函数的优化"""
        config = OptimizationConfig(method="grid", max_workers=1)
        optimizer = ParameterOptimizer(config)
        data = make_kline_df(200)

        def indicator_fn(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns([
                pl.col("close").rolling_mean(window_size=7).alias("ma7"),
            ])

        def strategy_fn(df: pl.DataFrame, bar_idx: int, params: dict) -> Optional[str]:
            if bar_idx < 10:
                return None
            if bar_idx == 50:
                return "LONG"
            if bar_idx == 100:
                return "CLOSE"
            return None

        ranges = [ParamRange("dummy", [1])]
        report = optimizer.optimize(data, strategy_fn, ranges, indicator_fn=indicator_fn)
        assert report.completed == 1

    def test_walk_forward(self):
        """样本内外分割"""
        config = OptimizationConfig(
            method="grid",
            max_workers=1,
            enable_walk_forward=True,
            train_ratio=0.7,
        )
        optimizer = ParameterOptimizer(config)
        data = make_kline_df(200)

        def strategy_fn(df: pl.DataFrame, bar_idx: int, params: dict) -> Optional[str]:
            if bar_idx == 20:
                return "LONG"
            if bar_idx == 40:
                return "CLOSE"
            return None

        ranges = [ParamRange("p", [1, 2])]
        report = optimizer.optimize(data, strategy_fn, ranges)

        assert report.completed == 2
        # 验证有样本外结果
        for r in report.results:
            assert r.oos_total_return is not None

    def test_progress_callback(self):
        """进度回调"""
        progress = []
        config = OptimizationConfig(method="grid", max_workers=1)
        optimizer = ParameterOptimizer(config)
        data = make_kline_df(100)

        def strategy_fn(df, bar_idx, params):
            return None

        ranges = [ParamRange("p", [1, 2, 3])]
        optimizer.optimize(
            data, strategy_fn, ranges,
            progress_callback=lambda done, total: progress.append((done, total)),
        )

        assert len(progress) == 3
        assert progress[-1] == (3, 3)

    def test_suggest_param_ranges(self):
        """推荐参数范围"""
        ranges = ParameterOptimizer.suggest_param_ranges("dual_ma")
        assert len(ranges) == 2
        assert ranges[0].name == "fast_period"
        assert ranges[1].name == "slow_period"

        # 未知策略返回空
        assert ParameterOptimizer.suggest_param_ranges("unknown") == []


# ─── 告警通知测试 ───


class TestAlertManager:
    @pytest.mark.asyncio
    async def test_fire_alert(self):
        """触发告警"""
        manager = AlertManager()
        manager.add_rule(AlertRule(
            name="test_alert",
            level=AlertLevel.WARNING,
            description="测试告警",
            cooldown_seconds=0,
        ))

        event = await manager.fire("test_alert", "测试消息", source="unit_test")

        assert event is not None
        assert event.level == AlertLevel.WARNING
        assert event.rule_name == "test_alert"
        assert event.source == "unit_test"
        assert len(manager.events) == 1

    @pytest.mark.asyncio
    async def test_cooldown(self):
        """冷却期过滤"""
        manager = AlertManager()
        manager.add_rule(AlertRule(
            name="cd_test",
            level=AlertLevel.INFO,
            cooldown_seconds=60,
        ))

        e1 = await manager.fire("cd_test", "first")
        e2 = await manager.fire("cd_test", "second")

        assert e1 is not None
        assert e2 is None  # 被冷却过滤

    @pytest.mark.asyncio
    async def test_disabled_rule(self):
        """禁用规则"""
        manager = AlertManager()
        manager.add_rule(AlertRule(
            name="disabled",
            level=AlertLevel.CRITICAL,
            enabled=False,
        ))

        event = await manager.fire("disabled", "should not fire")
        assert event is None

    @pytest.mark.asyncio
    async def test_level_override(self):
        """级别覆盖"""
        manager = AlertManager()

        event = await manager.fire("unknown_rule", "msg", level_override=AlertLevel.FATAL)
        assert event is not None
        assert event.level == AlertLevel.FATAL

    @pytest.mark.asyncio
    async def test_escalation(self):
        """告警升级"""
        manager = AlertManager()
        manager.add_rule(AlertRule(
            name="escalate_test",
            level=AlertLevel.WARNING,
            cooldown_seconds=0,
            escalate_after=3,
            escalate_to=AlertLevel.CRITICAL,
        ))

        events = []
        for i in range(3):
            e = await manager.fire("escalate_test", f"msg {i}")
            events.append(e)

        # 第 3 次触发应该升级
        assert events[0].level == AlertLevel.WARNING
        assert events[1].level == AlertLevel.WARNING
        assert events[2].level == AlertLevel.CRITICAL
        assert events[2].extra.get("escalated") is True

    @pytest.mark.asyncio
    async def test_alert_callback(self):
        """告警回调"""
        received = []
        manager = AlertManager()
        manager.on_alert(lambda e: received.append(e))

        await manager.fire_warning("callback test")

        assert len(received) == 1
        assert received[0].level == AlertLevel.WARNING

    @pytest.mark.asyncio
    async def test_get_events_filter(self):
        """事件查询过滤"""
        manager = AlertManager()

        await manager.fire_warning("warn 1", source="engine_a")
        await manager.fire_critical("crit 1", source="engine_b")
        await manager.fire_warning("warn 2", source="engine_a")

        # 按级别过滤
        warnings = manager.get_events(level=AlertLevel.WARNING)
        assert len(warnings) == 2

        # 按来源过滤
        engine_a = manager.get_events(source="engine_a")
        assert len(engine_a) == 2

    def test_stats(self):
        """统计信息"""
        manager = AlertManager()
        manager.setup_default_rules()

        stats = manager.get_stats()
        assert stats["rules_count"] == 8
        assert stats["total_events"] == 0

    def test_default_rules(self):
        """默认规则配置"""
        manager = AlertManager()
        manager.setup_default_rules()

        assert "ws_disconnect" in manager.rules
        assert "order_failed" in manager.rules
        assert "risk_triggered" in manager.rules
        assert "strategy_crash" in manager.rules
        assert manager.rules["ws_disconnect"].level == AlertLevel.CRITICAL

    @pytest.mark.asyncio
    async def test_acknowledge(self):
        """确认告警"""
        manager = AlertManager()
        event = await manager.fire_warning("ack test")

        assert event.acknowledged is False
        assert manager.acknowledge(event.alert_id) is True
        assert event.acknowledged is True
        assert manager.acknowledge("nonexist") is False


class TestNotificationChannels:
    def test_message_markdown(self):
        """消息 Markdown 格式化"""
        msg = Message(
            title="测试告警",
            content="这是一条测试消息",
            level="CRITICAL",
            extra={"策略": "DualMA", "品种": "BTCUSDT"},
        )

        md = msg.to_markdown()
        assert "测试告警" in md
        assert "CRITICAL" in md
        assert "策略: DualMA" in md
        assert msg.level_emoji == "🚨"

    def test_rate_limit(self):
        """频率限制"""
        channel = WebhookChannel(url="http://localhost:9999", rate_limit=3)

        # 模拟发送记录
        channel._send_times = [time.time(), time.time(), time.time()]
        assert channel._check_rate_limit() is False

        # 清空后应该可以发送
        channel._send_times = []
        assert channel._check_rate_limit() is True

    def test_channel_enable_disable(self):
        """渠道启用/禁用"""
        channel = WebhookChannel(url="http://localhost:9999")
        assert channel.enabled is True

        channel.enabled = False
        assert channel.enabled is False

    def test_dingtalk_sign(self):
        """钉钉签名生成"""
        channel = DingTalkChannel(
            webhook_url="https://oapi.dingtalk.com/robot/send?access_token=test",
            secret="SEC123456",
        )
        url = channel._sign_url()
        assert "timestamp=" in url
        assert "sign=" in url

    def test_feishu_sign(self):
        """飞书签名生成"""
        channel = FeishuChannel(
            webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
            secret="secret123",
        )
        sign_params = channel._gen_sign()
        assert "timestamp" in sign_params
        assert "sign" in sign_params
