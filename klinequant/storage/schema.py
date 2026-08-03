"""DuckDB Schema 定义与迁移

所有表使用 IF NOT EXISTS 保证幂等性。
字段命名与 protocol.types 保持一致，便于 ORM 映射。
时序数据表（klines/ticks）使用 timestamp 排序优化范围查询。
"""
from __future__ import annotations

# ─────────────────────────────────────────────
# 全部建表 DDL（IF NOT EXISTS，可重复执行）
# ─────────────────────────────────────────────

_KLINES_TABLE = """
CREATE TABLE IF NOT EXISTS klines (
    symbol        VARCHAR    NOT NULL,
    exchange      VARCHAR    NOT NULL,
    timeframe     VARCHAR    NOT NULL,
    timestamp     BIGINT     NOT NULL,   -- K 线开盘时间 Unix 毫秒 (UTC)
    open          DOUBLE     NOT NULL,
    high          DOUBLE     NOT NULL,
    low           DOUBLE     NOT NULL,
    close         DOUBLE     NOT NULL,
    volume        DOUBLE     NOT NULL,
    quote_volume  DOUBLE     NOT NULL,
    trade_count   INTEGER    NOT NULL,
    is_closed     BOOLEAN    NOT NULL DEFAULT FALSE,
    created_at    BIGINT     NOT NULL DEFAULT 0,

    PRIMARY KEY (symbol, exchange, timeframe, timestamp)
);
"""

_KLINES_INDEX = """
CREATE INDEX IF NOT EXISTS idx_klines_time_range
    ON klines (symbol, exchange, timeframe, timestamp);
"""

_TICKS_TABLE = """
CREATE TABLE IF NOT EXISTS ticks (
    symbol        VARCHAR    NOT NULL,
    exchange      VARCHAR    NOT NULL,
    timestamp     BIGINT     NOT NULL,   -- Unix 毫秒
    last_price    DOUBLE     NOT NULL,
    bid_price     DOUBLE     NOT NULL,
    bid_qty       DOUBLE     NOT NULL,
    ask_price     DOUBLE     NOT NULL,
    ask_qty       DOUBLE     NOT NULL,
    volume_24h    DOUBLE     NOT NULL,
    created_at    BIGINT     NOT NULL DEFAULT 0
);
"""

_TICKS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_ticks_time
    ON ticks (symbol, exchange, timestamp);
"""

_INDICATOR_VALUES_TABLE = """
CREATE TABLE IF NOT EXISTS indicator_values (
    symbol        VARCHAR    NOT NULL,
    exchange      VARCHAR    NOT NULL,
    timeframe     VARCHAR    NOT NULL,
    timestamp     BIGINT     NOT NULL,
    indicator     VARCHAR    NOT NULL,   -- 指标名称: MA/EMA/RSI/MACD/BOLL/ATR/KDJ/VWAP
    values        VARCHAR    NOT NULL,   -- JSON 序列化的指标值
    created_at    BIGINT     NOT NULL DEFAULT 0,

    PRIMARY KEY (symbol, exchange, timeframe, timestamp, indicator)
);
"""

_INDICATOR_INDEX = """
CREATE INDEX IF NOT EXISTS idx_indicator_time
    ON indicator_values (symbol, exchange, timeframe, indicator, timestamp);
"""

_ORDERS_TABLE = """
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR    NOT NULL PRIMARY KEY,
    strategy_id     VARCHAR    NOT NULL,
    symbol          VARCHAR    NOT NULL,
    exchange        VARCHAR    NOT NULL,
    side            VARCHAR    NOT NULL,       -- BUY / SELL
    order_type      VARCHAR    NOT NULL,       -- MARKET / LIMIT / STOP_LIMIT
    status          VARCHAR    NOT NULL,       -- OrderStatus 枚举值
    price           DOUBLE,
    quantity         DOUBLE     NOT NULL,
    filled_qty      DOUBLE     NOT NULL DEFAULT 0.0,
    filled_price    DOUBLE,
    fee             DOUBLE     NOT NULL DEFAULT 0.0,
    fee_asset       VARCHAR,
    leverage        INTEGER    NOT NULL DEFAULT 1,
    client_order_id VARCHAR,
    exchange_order_id VARCHAR,
    created_at      BIGINT     NOT NULL DEFAULT 0,
    updated_at      BIGINT     NOT NULL DEFAULT 0,
    extra           VARCHAR                    -- JSON 扩展字段
);
"""

_ORDERS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_orders_strategy
    ON orders (strategy_id, status);
CREATE INDEX IF NOT EXISTS idx_orders_time
    ON orders (created_at);
"""

_FILLS_TABLE = """
CREATE TABLE IF NOT EXISTS fills (
    fill_id         VARCHAR    NOT NULL PRIMARY KEY,
    order_id        VARCHAR    NOT NULL,
    strategy_id     VARCHAR    NOT NULL,
    symbol          VARCHAR    NOT NULL,
    exchange        VARCHAR    NOT NULL,
    side            VARCHAR    NOT NULL,
    price           DOUBLE     NOT NULL,
    quantity        DOUBLE     NOT NULL,
    fee             DOUBLE     NOT NULL DEFAULT 0.0,
    fee_asset       VARCHAR,
    timestamp       BIGINT     NOT NULL,
    created_at      BIGINT     NOT NULL DEFAULT 0
);
"""

_FILLS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_fills_order
    ON fills (order_id);
CREATE INDEX IF NOT EXISTS idx_fills_strategy_time
    ON fills (strategy_id, timestamp);
"""

_STRATEGIES_TABLE = """
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id     VARCHAR    NOT NULL PRIMARY KEY,
    name            VARCHAR    NOT NULL,
    version         VARCHAR    NOT NULL,
    config          VARCHAR    NOT NULL,       -- JSON 配置
    status          VARCHAR    NOT NULL DEFAULT 'STOPPED',
    symbols         VARCHAR    NOT NULL DEFAULT '[]', -- JSON 数组
    created_at      BIGINT     NOT NULL DEFAULT 0,
    updated_at      BIGINT     NOT NULL DEFAULT 0
);
"""

_RISK_LOGS_TABLE = """
CREATE TABLE IF NOT EXISTS risk_logs (
    log_id          VARCHAR    NOT NULL PRIMARY KEY,
    strategy_id     VARCHAR    NOT NULL,
    rule_name       VARCHAR    NOT NULL,
    level           VARCHAR    NOT NULL DEFAULT 'INFO',  -- INFO/WARN/ERROR/CRITICAL
    message         VARCHAR    NOT NULL,
    context         VARCHAR,                             -- JSON 上下文
    timestamp       BIGINT     NOT NULL,
    created_at      BIGINT     NOT NULL DEFAULT 0
);
"""

_RISK_LOGS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_risk_logs_time
    ON risk_logs (strategy_id, timestamp);
"""

_BACKTEST_RESULTS_TABLE = """
CREATE TABLE IF NOT EXISTS backtest_results (
    result_id       VARCHAR    NOT NULL PRIMARY KEY,
    strategy_id     VARCHAR    NOT NULL,
    config          VARCHAR    NOT NULL,       -- JSON 回测配置
    metrics         VARCHAR    NOT NULL,       -- JSON 绩效指标
    trades_count    INTEGER    NOT NULL DEFAULT 0,
    start_time      BIGINT     NOT NULL,
    end_time        BIGINT     NOT NULL,
    created_at      BIGINT     NOT NULL DEFAULT 0
);
"""

_AUDIT_LOGS_TABLE = """
CREATE TABLE IF NOT EXISTS audit_logs (
    log_id          VARCHAR    NOT NULL PRIMARY KEY,
    action          VARCHAR    NOT NULL,
    actor           VARCHAR    NOT NULL,
    target          VARCHAR,
    details         VARCHAR,                   -- JSON
    timestamp       BIGINT     NOT NULL,
    created_at      BIGINT     NOT NULL DEFAULT 0
);
"""

_SYS_CONFIG_TABLE = """
CREATE TABLE IF NOT EXISTS sys_config (
    key             VARCHAR    NOT NULL PRIMARY KEY,
    value           VARCHAR    NOT NULL,       -- JSON 值
    description     VARCHAR,
    updated_at      BIGINT     NOT NULL DEFAULT 0
);
"""


# ─────────────────────────────────────────────
# 迁移入口：按顺序执行全部 DDL
# ─────────────────────────────────────────────

_ALL_MIGRATIONS = [
    # 时序数据表
    _KLINES_TABLE,
    _KLINES_INDEX,
    _TICKS_TABLE,
    _TICKS_INDEX,
    _INDICATOR_VALUES_TABLE,
    _INDICATOR_INDEX,
    # 结构化业务表
    _ORDERS_TABLE,
    _ORDERS_INDEX,
    _FILLS_TABLE,
    _FILLS_INDEX,
    _STRATEGIES_TABLE,
    _RISK_LOGS_TABLE,
    _RISK_LOGS_INDEX,
    # 辅助表
    _BACKTEST_RESULTS_TABLE,
    _AUDIT_LOGS_TABLE,
    _SYS_CONFIG_TABLE,
]


def run_migrations() -> str:
    """返回所有迁移 SQL 的合并字符串，供 DuckDBManager.execute 使用。"""
    return "\n".join(_ALL_MIGRATIONS)


# 导出表名清单（供测试和监控使用）
TABLE_NAMES = [
    "klines", "ticks", "indicator_values",
    "orders", "fills", "strategies", "risk_logs",
    "backtest_results", "audit_logs", "sys_config",
]
