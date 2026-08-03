"""信号引擎单元测试

覆盖 SIG-T-001 ~ SIG-T-004：
    SIG-T-001: 金叉/死叉检测正确性
    SIG-T-002: AND/OR/NOT 组合逻辑真值表
    SIG-T-003: 信号冷却期：冷却内不重复触发
    SIG-T-004: 信号路由：三种模式正确分发
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import pytest

from core.signal_engine.engine import SignalEngine, SignalRoute
from core.signal_engine.rules import (
    AndRule,
    ComparisonRule,
    CrossoverRule,
    NotRule,
    OrRule,
    RuleBase,
    RuleResult,
    ThresholdRule,
)
from protocol.types import IndicatorValue, Signal, SignalDirection, SignalStrength


# ─── SIG-T-001: 金叉/死叉检测 ───

class TestCrossoverRule:
    def test_golden_cross(self):
        """金叉：fast 从下方穿越 slow → LONG"""
        rule = CrossoverRule(fast_key="DIF", slow_key="DEA")
        previous = {"DIF": 10.0, "DEA": 15.0}  # fast < slow
        current = {"DIF": 16.0, "DEA": 15.0}   # fast >= slow

        result = rule.evaluate(current, previous)
        assert result is not None
        assert result.direction == SignalDirection.LONG
        assert result.strength > 0

    def test_death_cross(self):
        """死叉：fast 从上方穿越 slow → SHORT"""
        rule = CrossoverRule(fast_key="DIF", slow_key="DEA")
        previous = {"DIF": 20.0, "DEA": 15.0}  # fast > slow
        current = {"DIF": 14.0, "DEA": 15.0}   # fast <= slow

        result = rule.evaluate(current, previous)
        assert result is not None
        assert result.direction == SignalDirection.SHORT

    def test_no_cross(self):
        """未交叉时不触发"""
        rule = CrossoverRule(fast_key="DIF", slow_key="DEA")

        # 持续在上方
        previous = {"DIF": 20.0, "DEA": 15.0}
        current = {"DIF": 18.0, "DEA": 15.0}
        assert rule.evaluate(current, previous) is None

        # 持续在下方
        previous = {"DIF": 10.0, "DEA": 15.0}
        current = {"DIF": 12.0, "DEA": 15.0}
        assert rule.evaluate(current, previous) is None

    def test_no_previous_data(self):
        """无前值时不触发"""
        rule = CrossoverRule(fast_key="DIF", slow_key="DEA")
        result = rule.evaluate({"DIF": 20.0, "DEA": 15.0}, None)
        assert result is None

    def test_missing_keys(self):
        """缺少 key 时不触发"""
        rule = CrossoverRule(fast_key="DIF", slow_key="DEA")
        result = rule.evaluate({"DIF": 20.0}, {"DIF": 10.0, "DEA": 15.0})
        assert result is None


class TestThresholdRule:
    def test_cross_upper(self):
        """上穿阈值 → SHORT（超买）"""
        rule = ThresholdRule(value_key="RSI", upper=70.0)
        previous = {"RSI": 65.0}
        current = {"RSI": 72.0}

        result = rule.evaluate(current, previous)
        assert result is not None
        assert result.direction == SignalDirection.SHORT

    def test_cross_lower(self):
        """下穿阈值 → LONG（超卖）"""
        rule = ThresholdRule(value_key="RSI", lower=30.0)
        previous = {"RSI": 35.0}
        current = {"RSI": 28.0}

        result = rule.evaluate(current, previous)
        assert result is not None
        assert result.direction == SignalDirection.LONG

    def test_no_cross(self):
        """未穿越阈值"""
        rule = ThresholdRule(value_key="RSI", upper=70.0, lower=30.0)
        # 在范围内
        previous = {"RSI": 50.0}
        current = {"RSI": 55.0}
        assert rule.evaluate(current, previous) is None


class TestComparisonRule:
    def test_greater_than(self):
        """> 运算符"""
        rule = ComparisonRule(
            left_key="MA7", right_key="MA25",
            operator=">", direction=SignalDirection.LONG,
        )
        result = rule.evaluate({"MA7": 105.0, "MA25": 100.0})
        assert result is not None
        assert result.direction == SignalDirection.LONG

    def test_less_than(self):
        """< 运算符"""
        rule = ComparisonRule(
            left_key="MA7", right_key="MA25",
            operator="<", direction=SignalDirection.SHORT,
        )
        result = rule.evaluate({"MA7": 95.0, "MA25": 100.0})
        assert result is not None
        assert result.direction == SignalDirection.SHORT

    def test_no_trigger(self):
        """条件不满足时不触发"""
        rule = ComparisonRule(
            left_key="MA7", right_key="MA25",
            operator=">", direction=SignalDirection.LONG,
        )
        result = rule.evaluate({"MA7": 95.0, "MA25": 100.0})
        assert result is None


# ─── SIG-T-002: AND/OR/NOT 组合逻辑 ───

class TestCompositeRules:
    def test_and_both_trigger(self):
        """AND: 两个子规则都触发 → 触发"""
        r1 = CrossoverRule("DIF", "DEA")
        r2 = ThresholdRule("RSI", lower=30.0)

        and_rule = AndRule([r1, r2])
        previous = {"DIF": 10.0, "DEA": 15.0, "RSI": 35.0}
        current = {"DIF": 16.0, "DEA": 15.0, "RSI": 28.0}

        result = and_rule.evaluate(current, previous)
        assert result is not None
        # 两个规则都产生 LONG → 方向一致
        assert result.direction == SignalDirection.LONG

    def test_and_one_missing(self):
        """AND: 一个子规则不触发 → 不触发"""
        r1 = CrossoverRule("DIF", "DEA")
        r2 = ThresholdRule("RSI", lower=30.0)

        and_rule = AndRule([r1, r2])
        previous = {"DIF": 20.0, "DEA": 15.0, "RSI": 35.0}  # r1: no cross
        current = {"DIF": 18.0, "DEA": 15.0, "RSI": 28.0}   # r2: cross lower

        result = and_rule.evaluate(current, previous)
        assert result is None

    def test_or_one_triggers(self):
        """OR: 任一子规则触发 → 触发"""
        r1 = CrossoverRule("DIF", "DEA")
        r2 = ThresholdRule("RSI", lower=30.0)

        or_rule = OrRule([r1, r2])
        previous = {"DIF": 20.0, "DEA": 15.0, "RSI": 35.0}
        current = {"DIF": 18.0, "DEA": 15.0, "RSI": 28.0}  # only r2 triggers

        result = or_rule.evaluate(current, previous)
        assert result is not None

    def test_or_none_triggers(self):
        """OR: 无子规则触发 → 不触发"""
        r1 = CrossoverRule("DIF", "DEA")
        r2 = ThresholdRule("RSI", lower=30.0)

        or_rule = OrRule([r1, r2])
        previous = {"DIF": 20.0, "DEA": 15.0, "RSI": 50.0}
        current = {"DIF": 18.0, "DEA": 15.0, "RSI": 55.0}

        result = or_rule.evaluate(current, previous)
        assert result is None

    def test_not_reverses_direction(self):
        """NOT: 反转方向 LONG → SHORT"""
        rule = CrossoverRule("DIF", "DEA")  # 金叉 → LONG
        not_rule = NotRule(rule)

        previous = {"DIF": 10.0, "DEA": 15.0}
        current = {"DIF": 16.0, "DEA": 15.0}

        result = not_rule.evaluate(current, previous)
        assert result is not None
        assert result.direction == SignalDirection.SHORT  # 反转

    def test_not_no_trigger(self):
        """NOT: 子规则不触发时 NOT 也不触发"""
        rule = CrossoverRule("DIF", "DEA")
        not_rule = NotRule(rule)

        previous = {"DIF": 20.0, "DEA": 15.0}
        current = {"DIF": 18.0, "DEA": 15.0}

        result = not_rule.evaluate(current, previous)
        assert result is None

    def test_and_conflicting_directions(self):
        """AND: 方向冲突时不触发"""
        r1 = CrossoverRule("DIF", "DEA")           # golden cross → LONG
        r2 = ThresholdRule("RSI", upper=70.0)      # cross upper → SHORT

        and_rule = AndRule([r1, r2])
        previous = {"DIF": 10.0, "DEA": 15.0, "RSI": 65.0}
        current = {"DIF": 16.0, "DEA": 15.0, "RSI": 72.0}

        result = and_rule.evaluate(current, previous)
        assert result is None  # LONG vs SHORT conflict


# ─── SIG-T-003: 信号冷却期 ───

class TestCooldown:
    def test_cooldown_prevents_repeat(self):
        """冷却期内不重复触发"""
        engine = SignalEngine(cooldown_seconds=60)
        rule = CrossoverRule("DIF", "DEA")
        engine.add_rule(rule, "MACD", "BTCUSDT")

        # 第一次触发
        iv1 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1000000, values={"DIF": 16.0, "DEA": 15.0},
        )
        # 需要先设置 previous
        iv0 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=999000, values={"DIF": 10.0, "DEA": 15.0},
        )
        engine.on_indicator_update(iv0)  # set previous
        signal1 = engine.on_indicator_update(iv1)  # golden cross
        assert signal1 is not None

        # 冷却期内再次触发（10秒后）
        iv2 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1010000, values={"DIF": 8.0, "DEA": 15.0},  # 回到下方
        )
        engine.on_indicator_update(iv2)  # set previous for next cross

        iv3 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1020000, values={"DIF": 16.0, "DEA": 15.0},  # 再次金叉
        )
        signal2 = engine.on_indicator_update(iv3)
        assert signal2 is None  # 被冷却期阻止

    def test_cooldown_expires(self):
        """冷却期过后可以再次触发"""
        engine = SignalEngine(cooldown_seconds=60)
        rule = CrossoverRule("DIF", "DEA")
        engine.add_rule(rule, "MACD", "BTCUSDT")

        # 第一次触发
        iv0 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1000000, values={"DIF": 10.0, "DEA": 15.0},
        )
        iv1 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1001000, values={"DIF": 16.0, "DEA": 15.0},
        )
        engine.on_indicator_update(iv0)
        signal1 = engine.on_indicator_update(iv1)
        assert signal1 is not None

        # 冷却期过后（120秒后）再次金叉
        iv2 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1050000, values={"DIF": 10.0, "DEA": 15.0},
        )
        engine.on_indicator_update(iv2)
        iv3 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1122000, values={"DIF": 16.0, "DEA": 15.0},  # > 60s 后
        )
        signal2 = engine.on_indicator_update(iv3)
        assert signal2 is not None  # 冷却期已过

    def test_clear_cooldowns(self):
        """清空冷却后可立即触发"""
        engine = SignalEngine(cooldown_seconds=600)
        rule = CrossoverRule("DIF", "DEA")
        engine.add_rule(rule, "MACD", "BTCUSDT")

        iv0 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1000000, values={"DIF": 10.0, "DEA": 15.0},
        )
        iv1 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1001000, values={"DIF": 16.0, "DEA": 15.0},
        )
        engine.on_indicator_update(iv0)
        engine.on_indicator_update(iv1)

        # 冷却中
        assert engine.check_cooldown(
            rule.name, "BTCUSDT", SignalDirection.LONG, 1002000
        )

        engine.clear_cooldowns()
        assert not engine.check_cooldown(
            rule.name, "BTCUSDT", SignalDirection.LONG, 1002000
        )


# ─── SIG-T-004: 信号路由 ───

class TestSignalRouting:
    def test_auto_route(self):
        """AUTO 路由：信号状态为 CONFIRMED"""
        engine = SignalEngine(cooldown_seconds=60, default_route=SignalRoute.AUTO)
        rule = CrossoverRule("DIF", "DEA")
        engine.add_rule(rule, "MACD", "BTCUSDT", route=SignalRoute.AUTO)

        iv0 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1000000, values={"DIF": 10.0, "DEA": 15.0},
        )
        iv1 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1001000, values={"DIF": 16.0, "DEA": 15.0},
        )
        engine.on_indicator_update(iv0)
        signal = engine.on_indicator_update(iv1)

        assert signal is not None
        assert signal.status == "CONFIRMED"

    def test_semi_auto_route(self):
        """SEMI_AUTO 路由：信号状态为 PENDING（需确认）"""
        engine = SignalEngine(cooldown_seconds=60)
        rule = CrossoverRule("DIF", "DEA")
        engine.add_rule(rule, "MACD", "BTCUSDT", route=SignalRoute.SEMI_AUTO)

        iv0 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1000000, values={"DIF": 10.0, "DEA": 15.0},
        )
        iv1 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1001000, values={"DIF": 16.0, "DEA": 15.0},
        )
        engine.on_indicator_update(iv0)
        signal = engine.on_indicator_update(iv1)

        assert signal is not None
        assert signal.status == "PENDING"

    def test_alert_only_route(self):
        """ALERT_ONLY 路由：默认模式"""
        engine = SignalEngine(cooldown_seconds=60, default_route=SignalRoute.ALERT_ONLY)
        assert engine.get_route("nonexistent") == SignalRoute.ALERT_ONLY

    def test_signal_subscriber(self):
        """信号订阅者收到通知"""
        engine = SignalEngine(cooldown_seconds=60)
        rule = CrossoverRule("DIF", "DEA")
        engine.add_rule(rule, "MACD", "BTCUSDT")

        received = []
        engine.subscribe_signals(lambda s: received.append(s))

        iv0 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1000000, values={"DIF": 10.0, "DEA": 15.0},
        )
        iv1 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1001000, values={"DIF": 16.0, "DEA": 15.0},
        )
        engine.on_indicator_update(iv0)
        engine.on_indicator_update(iv1)

        assert len(received) == 1
        assert isinstance(received[0], Signal)
        assert received[0].direction == SignalDirection.LONG

    def test_signal_strength_mapping(self):
        """信号强度映射正确"""
        engine = SignalEngine(cooldown_seconds=60)
        rule = CrossoverRule("DIF", "DEA")  # strength=0.7 → MEDIUM
        engine.add_rule(rule, "MACD", "BTCUSDT")

        iv0 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1000000, values={"DIF": 10.0, "DEA": 15.0},
        )
        iv1 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1001000, values={"DIF": 16.0, "DEA": 15.0},
        )
        engine.on_indicator_update(iv0)
        signal = engine.on_indicator_update(iv1)

        assert signal is not None
        assert signal.strength == SignalStrength.MEDIUM  # 0.7 → MEDIUM


# ─── 额外测试：RuleResult ───

class TestRuleResult:
    def test_strength_clamping(self):
        """强度值限制在 [0, 1]"""
        r = RuleResult("test", SignalDirection.LONG, strength=1.5)
        assert r.strength == 1.0

        r2 = RuleResult("test", SignalDirection.LONG, strength=-0.5)
        assert r2.strength == 0.0

    def test_repr(self):
        """字符串表示"""
        r = RuleResult("test_rule", SignalDirection.LONG, 0.8, "test reason")
        s = repr(r)
        assert "test_rule" in s
        assert "LONG" in s


class TestSignalEngineLifecycle:
    def test_start_stop(self):
        """引擎启停"""
        engine = SignalEngine()
        assert not engine.is_running
        engine.start()
        assert engine.is_running
        engine.stop()
        assert not engine.is_running

    def test_signal_count(self):
        """信号计数"""
        engine = SignalEngine(cooldown_seconds=60)
        rule = CrossoverRule("DIF", "DEA")
        engine.add_rule(rule, "MACD", "BTCUSDT")

        iv0 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1000000, values={"DIF": 10.0, "DEA": 15.0},
        )
        iv1 = IndicatorValue(
            indicator_name="MACD", symbol="BTCUSDT", timeframe="1m",
            timestamp=1001000, values={"DIF": 16.0, "DEA": 15.0},
        )
        engine.on_indicator_update(iv0)
        engine.on_indicator_update(iv1)
        assert engine.signal_count == 1
