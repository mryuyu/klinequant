---

**文档版本**：v2.0-win-native<font style="color:rgb(34, 34, 34);">  
</font>**运行平台**：Windows 10/11 (x64)<font style="color:rgb(34, 34, 34);">  
</font>**开发工具**：Qoder Desktop<font style="color:rgb(34, 34, 34);">  
</font>**数据库方案**：DuckDB（嵌入式分析）+ ClickHouse（时序存储）<font style="color:rgb(34, 34, 34);">  
</font>**服务管理**：Windows 原生服务（NSSM）+ PowerShell 脚本<font style="color:rgb(34, 34, 34);">  
</font>**适用对象**：专业量化交易者 / 小型团队（2-8人）

---

## 目录
1. 架构说明
2. Windows 开发环境搭建
3. 数据库架构设计（DuckDB + ClickHouse）
4. Windows 原生服务管理
5. 项目初始化（Windows 适配）
6. Qoder Rules 配置（更新）
7. 存储层开发指南
8. Windows 平台适配要点
9. 部署与运维（Windows）
10. Quest Spec 更新（存储相关）
11. 性能与调优
12. 迁移与备份

---

## 1. 架构变更说明
### 1.1 原方案 vs 新方案对比
| **组件** | ** 方案** | **理由** |
| :--- | :--- | :--- |
| 时序数据（K线/Tick） | ClickHouse | 列式存储，写入吞吐高 10x+，压缩率更优，天然适合时序 |
| 结构化数据（订单/策略/配置） | DuckDB | 嵌入式零运维，单文件部署，OLAP 查询极快，小团队无需独立数据库服务 |
| 缓存 | Redis+进程内缓存 + DuckDB  | 独立缓存服务，内存 dict + LRU 足够 |
| 回测数据分析 | DuckDB | 嵌入式分析引擎，直接读取 Parquet/CSV，无需数据搬运 |
| 服务管理 | Windows 原生服务（NSSM） | 零容器依赖，纯 Windows 运行，无许可证问题 |
| 消息队列 | ZMQ（已选）+ NATS（预留） | 不依赖 Redis |


### 1.2 数据库职责划分


```plain
1┌─────────────────────────────────────────────────────────────────┐
2│                        数据层架构                                 │
3├────────────────────────────┬────────────────────────────────────┤
4│        DuckDB              │         ClickHouse                 │
5│    （嵌入式 · 本地）        │      （服务端 · 时序）              │
6├────────────────────────────┼────────────────────────────────────┤
7│ • 订单 / 成交记录          │ • K 线数据（全周期）                │
8│ • 策略配置 / 参数          │ • Tick 逐笔数据                    │
9│ • 风控日志                 │ • 指标计算结果                      │
10│ • 回测结果 / 绩效报告      │ • 实时行情快照                      │
11│ • 用户配置 / 审计日志      │ • 高频时序分析                      │
12│ • 临时分析 / Ad-hoc 查询   │ • 历史数据归档                      │
13│ • Parquet 文件直接查询     │                                    │
14├────────────────────────────┼────────────────────────────────────┤
15│ 无需服务进程               │ 需要 ClickHouse Server             │
16│ 单文件 .duckdb            │ 默认端口 8123(HTTP) / 9000(TCP)    │
17│ 进程内访问，零延迟         │ 高吞吐写入，列式压缩               │
18└────────────────────────────┴────────────────────────────────────┘
```

### 1.3 缓存策略（推荐 Redis）
```plain
1# 小团队场景：进程内缓存完全足够
2# 使用 cachetools 实现 LRU/TTL 缓存
3
4from cachetools import TTLCache, LRUCache
5
6class CacheManager:
7    """进程内缓存管理器，也可使用Redis。"""
8
9    def __init__(self):
10        # K线快照缓存：最新 1000 根，TTL 60s
11        self.kline_cache: TTLCache = TTLCache(maxsize=5000, ttl=60)
12        # Ticker 缓存：TTL 5s
13        self.ticker_cache: TTLCache = TTLCache(maxsize=200, ttl=5)
14        # 账户余额缓存：TTL 10s
15        self.account_cache: TTLCache = TTLCache(maxsize=10, ttl=10)
16        # 品种信息缓存：LRU，长期有效
17        self.symbol_cache: LRUCache = LRUCache(maxsize=500)
```

---

## 2. Windows 开发环境搭建
### 2.1 必装软件清单
| **软件** | **版本** | **安装方式** | **用途** |
| :--- | :--- | :--- | :--- |
| Qoder Desktop | ≥ 1.0 | 官网下载 | 主 IDE |
| Python | 3.10 + (Windows installer) | [python.org](https://python.org/) | 后端运行时 |
| Node.js | 20 LTS+ | [nodejs.org](https://nodejs.org/)<br/> / winget | 前端构建 |
| pnpm | 9+ | `npm install -g pnpm` | 前端包管理 |
| Git for Windows | 2.44+ | [git-scm.com](https://git-scm.com/) | 版本控制 |
| ClickHouse | 24.3+ | 见 2.3 节 | 时序数据库 |
| DuckDB | 1.0+ | pip 安装（嵌入式） | 分析数据库 |
| NSSM | 2.24+ | [nssm.cc](https://nssm.cc/download) | Windows 服务管理 |
| Windows Terminal | 最新 | Microsoft Store | 终端 |
| Visual C++ Build Tools | 2022 | VS Installer | 编译 C 扩展 |
| Redis |Redis - Windows ||

### 2.2 使用 winget 批量安装
```plain
1# 以管理员身份运行 PowerShell
2winget install Python.Python.3.10+
3winget install OpenJS.NodeJS.LTS
4winget install Git.Git
5winget install Microsoft.WindowsTerminal
6winget install casey.just
7
8# Python 工具
9pip install uv
10uv --version
11
12# 前端
13npm install -g pnpm
```

### 2.3 ClickHouse 安装（Windows 原生）
```plain
1# 下载 ClickHouse Windows 版本
2# 访问 https://clickhouse.com/docs/en/install 下载最新 Windows 包
3# 或使用 curl：
4curl -O https://clickhouse.com/
5
6# 解压到 C:\clickhouse\
7# 目录结构：
8# C:\clickhouse\
9#   ├── clickhouse.exe        # 单二进制（含 server + client）
10#   └── config.xml            # 配置文件
11
12# 启动 ClickHouse Server
13cd C:\clickhouse
14.\clickhouse.exe server --config-file=config.xml
15
16# 验证（新终端）
17.\clickhouse.exe client --query "SELECT version()"
18# 输出：24.3.x.x
```

### 2.4 ClickHouse 配置（开发环境）
```plain
1<!-- C:\clickhouse\config.xml 关键配置 -->
2<clickhouse>
3    <logger>
4        <level>warning</level>
5        <log>C:\clickhouse\logs\clickhouse-server.log</log>
6        <errorlog>C:\clickhouse\logs\clickhouse-server.err.log</errorlog>
7    </logger>
8
9    <http_port>8123</http_port>
10    <tcp_port>9000</tcp_port>
11
12    <path>C:\clickhouse\data\</path>
13    <tmp_path>C:\clickhouse\tmp\</tmp_path>
14
15    <users_config>users.xml</users_config>
16
17    <!-- 开发环境：单用户，无密码 -->
18    <profiles>
19        <default>
20            <max_memory_usage>4000000000</max_memory_usage>  <!-- 4GB -->
21            <max_threads>4</max_threads>
22        </default>
23    </profiles>
24
25    <!-- 监听本地地址（开发环境） -->
26    <listen_host>127.0.0.1</listen_host>
27</clickhouse>
```

### 2.5 Python 环境（Windows）
```plain
1# 创建项目目录
2mkdir C:\Projects\klinequant
3cd C:\Projects\klinequant
4git init
5
6# 创建虚拟环境
7uv venv .venv --python 3.10+ 
8.venv\Scripts\activate    # Windows 激活方式
9
10# 初始化项目
11uv init --name klinequant
```



**pyproject.toml（Windows 适配版）：**

****

```plain
1[project]
2name = "klinequant"
3version = "0.1.0"
4requires-python = ">=3.12"
5dependencies = [
6    # === 通信 ===
7    "pyzmq>=26.0",
8    "msgpack>=1.0",
9    # === Web ===
10    "fastapi>=0.115",
11    "uvicorn[standard]>=0.30",
12    "websockets>=12.0",
13    # === 数据库 ===
14    "duckdb>=1.0",
15    "clickhouse-connect>=0.7",       # ClickHouse Python 驱动
16    "sqlalchemy>=2.0",               # 仅用于 DuckDB ORM（可选）
17    # === 缓存 ===
18    "redis>=5.0",
    "hiredis>=2.3",
19    # === HTTP ===
20    "httpx>=0.27",
21    "aiohttp>=3.9",
22    # === 数据 ===
23    "numpy>=1.26",
24    "polars>=1.0",                   # 主选：指标计算引擎（Rust 底层，多线程）
    "pandas>=2.2",                   # 备选：降级兼容（polars 不可用时回退）
25    "pyarrow>=16.0",                 # Parquet 读写（DuckDB 配合）
26    # === 指标 ===（基于 polars 自行实现，无需第三方指标库）
28    # === 工具 ===
29    "loguru>=0.7",
30    "pyyaml>=6.0",
31    "pydantic>=2.7",
32    "pydantic-settings>=2.3",
33    "python-jose[cryptography]>=3.3",
34    "passlib[bcrypt]>=1.7",
35    "apscheduler>=3.10",
36]
37
38[project.optional-dependencies]
39dev = [
40    "pytest>=8.0",
41    "pytest-asyncio>=0.23",
42    "pytest-cov>=5.0",
43    "httpx>=0.27",
44    "ruff>=0.4",
45    "mypy>=1.10",
46]
47
48[tool.ruff]
49target-version = "py310"
50line-length = 100
51
52[tool.pytest.ini_options]
53asyncio_mode = "auto"
54testpaths = ["tests"]
55# 在 conftest.py 中设置 event_loop_policy
```

✅ **指标计算引擎**：采用 `polars`（Rust 底层，多线程高性能）自行实现核心指标（MA/EMA/RSI/MACD/BOLL/ATR/KDJ/VWAP），`pandas` 作为降级备选。不使用 `ta-lib-bin`（C 库在 Windows 编译困难），也不依赖 `pandas-ta`（与 polars 生态不兼容）。polars 本身为 Rust 实现，后期可直接迁移 Rust 原生指标计算模块，实现零成本热路径重写。

---

## 3. 数据库架构设计（DuckDB + ClickHouse）
### 3.1 ClickHouse — 时序数据层
#### 3.1.1 数据库与表结构


```plain
1-- 创建数据库
2CREATE DATABASE IF NOT EXISTS klinequant;
3
4-- ═══════════════════════════════════════════
5-- K 线表（核心时序表）
6-- ═══════════════════════════════════════════
7CREATE TABLE klinequant.klines (
8    timestamp   DateTime64(3, 'UTC'),   -- 毫秒精度
9    symbol      LowCardinality(String),  -- 低基数字符串优化
10    exchange    LowCardinality(String),
11    timeframe   LowCardinality(String),
12    open        Decimal64(8),
13    high        Decimal64(8),
14    low         Decimal64(8),
15    close       Decimal64(8),
16    volume      Decimal64(8),
17    quote_volume Decimal64(8),
18    trade_count UInt32,
19    is_closed   UInt8
20)
21ENGINE = MergeTree()
22PARTITION BY (exchange, symbol, toYYYYMM(timestamp))
23ORDER BY (symbol, exchange, timeframe, timestamp)
24TTL timestamp + INTERVAL 5 YEAR DELETE   -- 5 年自动清理（可配置）
25SETTINGS index_granularity = 8192;
26
27-- 物化视图：自动聚合 1m → 1h（可选，减少查询压力）
28CREATE MATERIALIZED VIEW klinequant.klines_1h_mv
29ENGINE = AggregatingMergeTree()
30PARTITION BY (symbol, toYYYYMM(timestamp))
31ORDER BY (symbol, timeframe, timestamp)
32AS SELECT
33    toStartOfHour(timestamp) AS timestamp,
34    symbol,
35    exchange,
36    '1h' AS timeframe,
37    argMinState(open, timestamp) AS open,
38    maxState(high) AS high,
39    minState(low) AS low,
40    argMaxState(close, timestamp) AS close,
41    sumState(volume) AS volume,
42    sumState(quote_volume) AS quote_volume,
43    sumState(trade_count) AS trade_count,
44    1 AS is_closed
45FROM klinequant.klines
46WHERE timeframe = '1m' AND is_closed = 1
47GROUP BY timestamp, symbol, exchange;
48
49-- ═══════════════════════════════════════════
50-- Tick 数据表（高频）
51-- ═══════════════════════════════════════════
52CREATE TABLE klinequant.ticks (
53    timestamp   DateTime64(3, 'UTC'),
54    symbol      LowCardinality(String),
55    exchange    LowCardinality(String),
56    last_price  Decimal64(8),
57    bid_price   Decimal64(8),
58    bid_qty     Decimal64(8),
59    ask_price   Decimal64(8),
60    ask_qty     Decimal64(8),
61    volume_24h  Decimal64(8)
62)
63ENGINE = MergeTree()
64PARTITION BY (symbol, toYYYYMMDD(timestamp))  -- 按天分区（Tick 量大）
65ORDER BY (symbol, timestamp)
66TTL timestamp + INTERVAL 90 DAY DELETE;       -- Tick 保留 90 天
67
68-- ═══════════════════════════════════════════
69-- 指标值表
70-- ═══════════════════════════════════════════
71CREATE TABLE klinequant.indicator_values (
72    timestamp       DateTime64(3, 'UTC'),
73    symbol          LowCardinality(String),
74    timeframe       LowCardinality(String),
75    indicator_name  LowCardinality(String),
76    params_hash     String,              -- 参数哈希（区分 MA(7) vs MA(25)）
77    values          String,              -- JSON 格式存储多值指标
78    PRIMARY KEY (symbol, timeframe, indicator_name, params_hash, timestamp)
79)
80ENGINE = MergeTree()
81ORDER BY (symbol, timeframe, indicator_name, params_hash, timestamp)
82TTL timestamp + INTERVAL 1 YEAR DELETE;
83
84-- ═══════════════════════════════════════════
85-- 行情快照（ReplacingMergeTree，仅保留最新）
86-- ═══════════════════════════════════════════
87CREATE TABLE klinequant.market_snapshot (
88    symbol      LowCardinality(String),
89    exchange    LowCardinality(String),
90    timestamp   DateTime64(3, 'UTC'),
91    last_price  Decimal64(8),
92    bid_price   Decimal64(8),
93    ask_price   Decimal64(8),
94    volume_24h  Decimal64(8),
95    change_pct  Float32
96)
97ENGINE = ReplacingMergeTree(timestamp)
98ORDER BY (symbol, exchange)
99SETTINGS index_granularity = 256;
```

#### 3.1.2 ClickHouse 写入策略


```plain
1# 批量写入，非逐条插入（ClickHouse 最佳实践）
2class ClickHouseWriter:
3    """ClickHouse 批量写入管理器。"""
4
5    BATCH_SIZE = 1000          # 每批最大条数
6    FLUSH_INTERVAL_SEC = 2.0   # 最大等待时间
7
8    def __init__(self, client: clickhouse_connect.driver.Client):
9        self._client = client
10        self._buffer: dict[str, list] = defaultdict(list)
11        self._lock = asyncio.Lock()
12        self._flush_task: Optional[asyncio.Task] = None
13
14    async def start(self):
15        self._flush_task = asyncio.create_task(self._flush_loop())
16
17    async def write_kline(self, kline: Kline) -> None:
18        """缓冲 K 线，批量写入。"""
19        async with self._lock:
20            self._buffer["klines"].append(self._kline_to_row(kline))
21            if len(self._buffer["klines"]) >= self.BATCH_SIZE:
22                await self._flush_table("klines")
23
24    async def _flush_loop(self):
25        """定时刷新缓冲区。"""
26        while True:
27            await asyncio.sleep(self.FLUSH_INTERVAL_SEC)
28            async with self._lock:
29                for table in list(self._buffer.keys()):
30                    if self._buffer[table]:
31                        await self._flush_table(table)
32
33    async def _flush_table(self, table: str):
34        rows = self._buffer.pop(table, [])
35        if not rows:
36            return
37        # 使用 clickhouse-connect 的 insert 方法（同步，需 run_in_executor）
38        loop = asyncio.get_event_loop()
39        await loop.run_in_executor(
40            None,
41            self._client.insert,
42            f"klinequant.{table}",
43            rows,
44            column_names=self._get_columns(table),
45        )
```

### 3.2 DuckDB — 结构化数据 + 分析层
#### 3.2.1 数据库文件布局


```plain
1data/
2├── klinequant.duckdb          # 主数据库（订单、策略、配置）
3├── klinequant.duckdb.wal      # WAL 文件（自动管理）
4├── backtest/                  # 回测结果（独立文件，避免锁竞争）
5│   ├── bt_20240101_001.duckdb
6│   └── bt_20240102_002.duckdb
7└── exports/                   # Parquet 导出（ClickHouse → DuckDB 分析）
8    ├── klines_1h_btc.parquet
9    └── trades_2024.parquet
```

#### 3.2.2 DuckDB 表结构


```plain
1# storage/duckdb_schema.py
2"""DuckDB 数据库初始化与 Schema 定义。"""
3
4DUCKDB_SCHEMA = """
5-- ═══════════════════════════════════════════
6-- 订单表
7-- ═══════════════════════════════════════════
8CREATE TABLE IF NOT EXISTS orders (
9    order_id        VARCHAR PRIMARY KEY,
10    exchange_oid    VARCHAR,
11    strategy_id     VARCHAR,
12    symbol          VARCHAR NOT NULL,
13    exchange        VARCHAR NOT NULL,
14    side            VARCHAR NOT NULL,          -- BUY / SELL
15    order_type      VARCHAR NOT NULL,          -- MARKET / LIMIT / STOP_LIMIT
16    price           DECIMAL(20,8),
17    quantity        DECIMAL(20,8) NOT NULL,
18    filled_qty      DECIMAL(20,8) DEFAULT 0,
19    avg_fill_price  DECIMAL(20,8),
20    status          VARCHAR NOT NULL,
21    fee             DECIMAL(20,8) DEFAULT 0,
22    fee_currency    VARCHAR,
23    client_oid      VARCHAR UNIQUE,
24    cancel_reason   VARCHAR,
25    metadata        JSON,
26    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
27    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL,
28    filled_at       TIMESTAMP WITH TIME ZONE
29);
30
31-- ═══════════════════════════════════════════
32-- 成交记录表
33-- ═══════════════════════════════════════════
34CREATE TABLE IF NOT EXISTS fills (
35    fill_id         VARCHAR PRIMARY KEY,
36    order_id        VARCHAR NOT NULL,
37    symbol          VARCHAR NOT NULL,
38    side            VARCHAR NOT NULL,
39    price           DECIMAL(20,8) NOT NULL,
40    quantity        DECIMAL(20,8) NOT NULL,
41    fee             DECIMAL(20,8),
42    fee_currency    VARCHAR,
43    filled_at       TIMESTAMP WITH TIME ZONE NOT NULL
44);
45
46-- ═══════════════════════════════════════════
47-- 策略配置表
48-- ═══════════════════════════════════════════
49CREATE TABLE IF NOT EXISTS strategies (
50    strategy_id     VARCHAR PRIMARY KEY,
51    name            VARCHAR NOT NULL,
52    module_path     VARCHAR NOT NULL,
53    params          JSON NOT NULL DEFAULT '{}',
54    status          VARCHAR NOT NULL DEFAULT 'STOPPED',
55    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
56    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL
57);
58
59-- ═══════════════════════════════════════════
60-- 风控日志表
61-- ═══════════════════════════════════════════
62CREATE TABLE IF NOT EXISTS risk_logs (
63    log_id          VARCHAR PRIMARY KEY,
64    timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
65    order_id        VARCHAR,
66    strategy_id     VARCHAR,
67    rule_id         VARCHAR NOT NULL,
68    result          VARCHAR NOT NULL,          -- PASS / REJECT
69    detail          VARCHAR,
70    account_snapshot JSON
71);
72
73-- ═══════════════════════════════════════════
74-- 回测结果表
75-- ═══════════════════════════════════════════
76CREATE TABLE IF NOT EXISTS backtest_results (
77    task_id         VARCHAR PRIMARY KEY,
78    strategy_id     VARCHAR NOT NULL,
79    params          JSON,
80    symbol          VARCHAR,
81    timeframe       VARCHAR,
82    start_date      DATE,
83    end_date        DATE,
84    initial_capital DECIMAL(20,8),
85    final_capital   DECIMAL(20,8),
86    metrics         JSON,                      -- 夏普、回撤等
87    created_at      TIMESTAMP WITH TIME ZONE NOT NULL
88);
89
90-- ═══════════════════════════════════════════
91-- 审计日志表
92-- ═══════════════════════════════════════════
93CREATE TABLE IF NOT EXISTS audit_logs (
94    log_id          VARCHAR PRIMARY KEY,
95    timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
96    user_id         VARCHAR,
97    action          VARCHAR NOT NULL,
98    resource_type   VARCHAR,
99    resource_id     VARCHAR,
100    detail          JSON
101);
102
103-- ═══════════════════════════════════════════
104-- 系统配置表（KV 存储）
105-- ═══════════════════════════════════════════
106CREATE TABLE IF NOT EXISTS sys_config (
107    key             VARCHAR PRIMARY KEY,
108    value           JSON NOT NULL,
109    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL
110);
111
112-- 索引
113CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
114CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
115CREATE INDEX IF NOT EXISTS idx_orders_strategy ON orders(strategy_id);
116CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
117CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id);
118CREATE INDEX IF NOT EXISTS idx_risk_logs_time ON risk_logs(timestamp);
119"""
```

#### 3.2.3 DuckDB 连接管理


```plain
1# storage/duckdb_manager.py
2"""DuckDB 连接管理器。
3
4DuckDB 特性：
5- 单进程写入，多进程只读
6- 写操作需要独占锁
7- 适合 OLAP 查询，不适合高并发 OLTP
8"""
9from __future__ import annotations
10
11import asyncio
12from pathlib import Path
13from typing import Any, Optional
14
15import duckdb
16from loguru import logger
17
18
19class DuckDBManager:
20    """DuckDB 连接管理器（单例）。
21
22    写操作通过 asyncio.Lock 串行化。
23    读操作可并发（DuckDB 支持多读）。
24    """
25
26    def __init__(self, db_path: str | Path = "data/klinequant.duckdb"):
27        self._db_path = Path(db_path)
28        self._db_path.parent.mkdir(parents=True, exist_ok=True)
29        self._conn: Optional[duckdb.DuckDBPyConnection] = None
30        self._write_lock = asyncio.Lock()
31        self._logger = logger.bind(component="duckdb")
32
33    async def initialize(self) -> None:
34        """初始化连接并创建 Schema。"""
35        loop = asyncio.get_event_loop()
36        self._conn = await loop.run_in_executor(
37            None, duckdb.connect, str(self._db_path)
38        )
39        from storage.duckdb_schema import DUCKDB_SCHEMA
40        await loop.run_in_executor(None, self._conn.execute, DUCKDB_SCHEMA)
41        self._logger.info("DuckDB initialized", path=str(self._db_path))
42
43    async def execute_write(self, sql: str, params: list[Any] | None = None) -> None:
44        """写操作（串行化）。"""
45        async with self._write_lock:
46            loop = asyncio.get_event_loop()
47            await loop.run_in_executor(
48                None, self._conn.execute, sql, params or []
49            )
50
51    async def execute_read(self, sql: str, params: list[Any] | None = None) -> list:
52        """读操作（可并发）。"""
53        loop = asyncio.get_event_loop()
54        result = await loop.run_in_executor(
55            None, self._conn.execute, sql, params or []
56        )
57        return await loop.run_in_executor(None, result.fetchall)
58
59    async def query_df(self, sql: str) -> "pd.DataFrame":
60        """返回 DataFrame（分析查询）。"""
61        loop = asyncio.get_event_loop()
62        result = await loop.run_in_executor(None, self._conn.execute, sql)
63        return await loop.run_in_executor(None, result.fetchdf)
64
65    async def close(self) -> None:
66        if self._conn:
67            loop = asyncio.get_event_loop()
68            await loop.run_in_executor(None, self._conn.close)
69            self._logger.info("DuckDB closed")
```

### 3.3 ClickHouse 连接管理


```plain
1# storage/clickhouse_manager.py
2"""ClickHouse 异步连接管理器。"""
3from __future__ import annotations
4
5import asyncio
6from typing import Any, Optional
7
8import clickhouse_connect
9from clickhouse_connect.driver import Client
10from loguru import logger
11
12
13class ClickHouseManager:
14    """ClickHouse 连接管理。
15
16    clickhouse-connect 是同步驱动，通过 run_in_executor 异步化。
17    写入使用批量 insert，读取使用 query。
18    """
19
20    def __init__(
21        self,
22        host: str = "localhost",
23        port: int = 8123,
24        database: str = "klinequant",
25        username: str = "default",
26        password: str = "",
27    ):
28        self._host = host
29        self._port = port
30        self._database = database
31        self._username = username
32        self._password = password
33        self._client: Optional[Client] = None
34        self._logger = logger.bind(component="clickhouse")
35
36    async def initialize(self) -> None:
37        loop = asyncio.get_event_loop()
38        self._client = await loop.run_in_executor(
39            None,
40            clickhouse_connect.get_client,
41            self._host,
42            self._port,
43            self._username,
44            self._password,
45        )
46        result = await loop.run_in_executor(
47            None, self._client.query, "SELECT 1"
48        )
49        self._logger.info("ClickHouse connected", host=self._host)
50
51    async def query(self, sql: str, parameters: dict | None = None) -> list:
52        """执行查询。"""
53        loop = asyncio.get_event_loop()
54        result = await loop.run_in_executor(
55            None,
56            lambda: self._client.query(sql, parameters=parameters or {}),
57        )
58        return result.result_rows
59
60    async def query_df(self, sql: str, parameters: dict | None = None):
61        """返回 DataFrame。"""
62        loop = asyncio.get_event_loop()
63        result = await loop.run_in_executor(
64            None,
65            lambda: self._client.query_df(sql, parameters=parameters or {}),
66        )
67        return result
68
69    async def insert(self, table: str, data: list[list], column_names: list[str]) -> None:
70        """批量插入。"""
71        loop = asyncio.get_event_loop()
72        await loop.run_in_executor(
73            None,
74            lambda: self._client.insert(
75                f"{self._database}.{table}",
76                data,
77                column_names=column_names,
78            ),
79        )
80
81    async def execute(self, sql: str) -> None:
82        """执行 DDL / 命令。"""
83        loop = asyncio.get_event_loop()
84        await loop.run_in_executor(None, self._client.command, sql)
85
86    async def close(self) -> None:
87        if self._client:
88            loop = asyncio.get_event_loop()
89            await loop.run_in_executor(None, self._client.close)
```

### 3.4 数据访问层（Repository）


```plain
1# storage/repositories/kline_repo.py
2"""K 线数据仓库（ClickHouse）。"""
3from __future__ import annotations
4
5from decimal import Decimal
6from typing import Optional
7
8from protocol.types import Kline
9from storage.clickhouse_manager import ClickHouseManager
10
11
12class KlineRepository:
13    """K 线数据访问。"""
14
15    def __init__(self, ch: ClickHouseManager):
16        self._ch = ch
17
18    async def save_batch(self, klines: list[Kline]) -> None:
19        """批量写入 K 线。"""
20        if not klines:
21            return
22        rows = [
23            [
24                k.timestamp / 1000,  # ms → DateTime64(3)
25                k.symbol,
26                k.exchange,
27                k.timeframe,
28                float(k.open),
29                float(k.high),
30                float(k.low),
31                float(k.close),
32                float(k.volume),
33                float(k.quote_volume),
34                k.trade_count,
35                int(k.is_closed),
36            ]
37            for k in klines
38        ]
39        columns = [
40            "timestamp", "symbol", "exchange", "timeframe",
41            "open", "high", "low", "close",
42            "volume", "quote_volume", "trade_count", "is_closed",
43        ]
44        await self._ch.insert("klines", rows, columns)
45
46    async def get_klines(
47        self,
48        symbol: str,
49        exchange: str,
50        timeframe: str,
51        start_ms: Optional[int] = None,
52        end_ms: Optional[int] = None,
53        limit: int = 200,
54    ) -> list[Kline]:
55        """查询历史 K 线。"""
56        sql = """
57            SELECT timestamp, symbol, exchange, timeframe,
58                   open, high, low, close, volume, quote_volume,
59                   trade_count, is_closed
60            FROM klinequant.klines
61            WHERE symbol = {symbol:String}
62              AND exchange = {exchange:String}
63              AND timeframe = {timeframe:String}
64              {start_filter:String}
65              {end_filter:String}
66            ORDER BY timestamp DESC
67            LIMIT {limit:UInt32}
68        """
69        params = {
70            "symbol": symbol,
71            "exchange": exchange,
72            "timeframe": timeframe,
73            "start_filter": f"AND timestamp >= fromUnixTimestamp64Milli({start_ms})" if start_ms else "",
74            "end_filter": f"AND timestamp <= fromUnixTimestamp64Milli({end_ms})" if end_ms else "",
75            "limit": limit,
76        }
77        rows = await self._ch.query(sql, params)
78        return [self._row_to_kline(r) for r in reversed(rows)]
79
80    def _row_to_kline(self, row: tuple) -> Kline:
81        return Kline(
82            symbol=row[1],
83            exchange=row[2],
84            timeframe=row[3],
85            timestamp=int(row[0].timestamp() * 1000),
86            open=Decimal(str(row[4])),
87            high=Decimal(str(row[5])),
88            low=Decimal(str(row[6])),
89            close=Decimal(str(row[7])),
90            volume=Decimal(str(row[8])),
91            quote_volume=Decimal(str(row[9])),
92            trade_count=row[10],
93            is_closed=bool(row[11]),
94        )
```



```plain
1# storage/repositories/order_repo.py
2"""订单数据仓库（DuckDB）。"""
3from __future__ import annotations
4
5from decimal import Decimal
6from typing import Optional
7
8from protocol.messages import OrderStatus
9from protocol.types import Order
10from storage.duckdb_manager import DuckDBManager
11
12
13class OrderRepository:
14    """订单数据访问（DuckDB）。"""
15
16    def __init__(self, db: DuckDBManager):
17        self._db = db
18
19    async def save(self, order: Order) -> None:
20        """插入或更新订单。"""
21        await self._db.execute_write(
22            """
23            INSERT OR REPLACE INTO orders (
24                order_id, exchange_oid, strategy_id, symbol, exchange,
25                side, order_type, price, quantity, filled_qty,
26                avg_fill_price, status, fee, fee_currency,
27                client_oid, cancel_reason, metadata,
28                created_at, updated_at, filled_at
29            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
30            """,
31            [
32                order.order_id,
33                order.exchange_order_id,
34                order.strategy_id,
35                order.symbol,
36                order.exchange,
37                order.side.value,
38                order.order_type.value,
39                float(order.price) if order.price else None,
40                float(order.quantity),
41                float(order.filled_quantity),
42                float(order.avg_fill_price) if order.avg_fill_price else None,
43                order.status.value,
44                float(order.fee),
45                order.fee_currency,
46                order.client_order_id,
47                order.cancel_reason,
48                str(order.metadata) if order.metadata else None,
49                order.created_at,
50                order.updated_at,
51                order.filled_at,
52            ],
53        )
54
55    async def get_by_id(self, order_id: str) -> Optional[Order]:
56        rows = await self._db.execute_read(
57            "SELECT * FROM orders WHERE order_id = ?", [order_id]
58        )
59        return self._row_to_order(rows[0]) if rows else None
60
61    async def get_open_orders(self, symbol: Optional[str] = None) -> list[Order]:
62        sql = "SELECT * FROM orders WHERE status IN ('SUBMITTED', 'PARTIAL_FILLED')"
63        params = []
64        if symbol:
65            sql += " AND symbol = ?"
66            params.append(symbol)
67        sql += " ORDER BY created_at DESC"
68        rows = await self._db.execute_read(sql, params)
69        return [self._row_to_order(r) for r in rows]
70
71    async def update_status(self, order_id: str, status: OrderStatus, **kwargs) -> None:
72        sets = ["status = ?", "updated_at = ?"]
73        params = [status.value, kwargs.get("updated_at")]
74        if "filled_qty" in kwargs:
75            sets.append("filled_qty = ?")
76            params.append(float(kwargs["filled_qty"]))
77        if "avg_fill_price" in kwargs:
78            sets.append("avg_fill_price = ?")
79            params.append(float(kwargs["avg_fill_price"]))
80        params.append(order_id)
81        await self._db.execute_write(
82            f"UPDATE orders SET {', '.join(sets)} WHERE order_id = ?",
83            params,
84        )
```

### 3.5 跨库查询（ClickHouse → DuckDB 分析）


```plain
1# storage/analytics_bridge.py
2"""跨库分析桥接：从 ClickHouse 导出数据到 DuckDB 进行分析。"""
3from __future__ import annotations
4
5import tempfile
6from pathlib import Path
7
8from storage.clickhouse_manager import ClickHouseManager
9from storage.duckdb_manager import DuckDBManager
10
11
12class AnalyticsBridge:
13    """ClickHouse 时序数据 → DuckDB 分析。
14
15    典型场景：回测时从 ClickHouse 拉取历史 K 线，
16    在 DuckDB 中进行复杂分析查询。
17    """
18
19    def __init__(self, ch: ClickHouseManager, duck: DuckDBManager):
20        self._ch = ch
21        self._duck = duck
22
23    async def export_klines_to_parquet(
24        self, symbol: str, timeframe: str, start: str, end: str, output: Path
25    ) -> Path:
26        """从 ClickHouse 导出 K 线为 Parquet 文件。"""
27        df = await self._ch.query_df(f"""
28            SELECT * FROM klinequant.klines
29            WHERE symbol = '{symbol}'
30              AND timeframe = '{timeframe}'
31              AND timestamp >= '{start}'
32              AND timestamp <= '{end}'
33            ORDER BY timestamp
34        """)
35        df.to_parquet(output, index=False)
36        return output
37
38    async def analyze_in_duckdb(self, parquet_path: Path, query: str):
39        """在 DuckDB 中直接查询 Parquet 文件（零拷贝）。"""
40        sql = query.replace("{parquet}", str(parquet_path))
41        return await self._duck.query_df(sql)
42
43    # 使用示例：
44    # bridge.analyze_in_duckdb(
45    #     Path("data/exports/klines.parquet"),
46    #     "SELECT date_trunc('month', timestamp) as m, avg(close) FROM '{parquet}' GROUP BY m"
47    # )
```

---

## 4. Windows 原生服务管理
### 4.1 架构总览（纯 Windows）


```plain
1┌─────────────────────────────────────────────────────────┐
2│  Windows 主机（全部原生进程）                             │
3│                                                         │
4│  ┌─────────────────────────────────────────────────┐    │
5│  │  Python 引擎进程                                 │    │
6│  │  • MarketEngine（行情引擎）                      │    │
7│  │  • IndicatorEngine（指标引擎）                   │    │
8│  │  • SignalEngine（信号引擎）                      │    │
9│  │  • TradeEngine（交易引擎）                       │    │
10│  │  • Gateway（API 网关）                           │    │
11│  └─────────────────────────────────────────────────┘    │
12│                                                         │
13│  ┌─────────────────────────────────────────────────┐    │
14│  │  DuckDB（嵌入式，无需服务进程）                   │    │
15│  │  • 单文件 data/klinequant.duckdb                │    │
16│  └─────────────────────────────────────────────────┘    │
17│                                                         │
18│  ┌─────────────────────────────────────────────────┐    │
19│  │  ClickHouse Server（Windows 原生二进制）          │    │
20│  │  • C:\clickhouse\clickhouse.exe server          │    │
21│  │  • 端口 8123(HTTP) / 9000(TCP)                  │    │
22│  └─────────────────────────────────────────────────┘    │
23│                                                         │
24│  ┌─────────────────────────────────────────────────┐    │
25│  │  Node.js（前端 dev server / 构建）               │    │
26│  └─────────────────────────────────────────────────┘    │
27└─────────────────────────────────────────────────────────┘
```

### 4.2 使用 NSSM 注册 Windows 服务
使用 **NSSM**（Non-Sucking Service Manager）将各组件注册为 Windows 服务，实现开机自启、崩溃重启、日志管理：



```plain
1# 下载 NSSM：https://nssm.cc/download
2# 解压到 C:\tools\nssm\
3
4# ═══ 注册 ClickHouse 为 Windows 服务 ═══
5C:\tools\nssm\nssm.exe install KQ-ClickHouse "C:\clickhouse\clickhouse.exe" "server --config-file=C:\clickhouse\config.xml"
6C:\tools\nssm\nssm.exe set KQ-ClickHouse AppDirectory "C:\clickhouse"
7C:\tools\nssm\nssm.exe set KQ-ClickHouse AppStdout "C:\clickhouse\logs\service_stdout.log"
8C:\tools\nssm\nssm.exe set KQ-ClickHouse AppStderr "C:\clickhouse\logs\service_stderr.log"
9C:\tools\nssm\nssm.exe set KQ-ClickHouse AppRotateFiles 1
10C:\tools\nssm\nssm.exe set KQ-ClickHouse AppRotateBytes 10485760
11C:\tools\nssm\nssm.exe start KQ-ClickHouse
12
13# ═══ 注册 Python 引擎为服务 ═══
14C:\tools\nssm\nssm.exe install KQ-MarketEngine "C:\Projects\klinequant\.venv\Scripts\python.exe" "-m core.market_engine.engine"
15C:\tools\nssm\nssm.exe set KQ-MarketEngine AppDirectory "C:\Projects\klinequant"
16C:\tools\nssm\nssm.exe set KQ-MarketEngine AppEnvironmentExtra "KQ_MODE=paper"
17C:\tools\nssm\nssm.exe set KQ-MarketEngine AppStdout "C:\Projects\klinequant\logs\market_stdout.log"
18C:\tools\nssm\nssm.exe set KQ-MarketEngine AppStderr "C:\Projects\klinequant\logs\market_stderr.log"
19C:\tools\nssm\nssm.exe start KQ-MarketEngine
20
21# ═══ 注册其他引擎（同理）═══
22C:\tools\nssm\nssm.exe install KQ-IndicatorEngine "C:\Projects\klinequant\.venv\Scripts\python.exe" "-m core.indicator_engine.engine"
23C:\tools\nssm\nssm.exe set KQ-IndicatorEngine AppDirectory "C:\Projects\klinequant"
24C:\tools\nssm\nssm.exe start KQ-IndicatorEngine
25
26C:\tools\nssm\nssm.exe install KQ-SignalEngine "C:\Projects\klinequant\.venv\Scripts\python.exe" "-m core.signal_engine.engine"
27C:\tools\nssm\nssm.exe set KQ-SignalEngine AppDirectory "C:\Projects\klinequant"
28C:\tools\nssm\nssm.exe start KQ-SignalEngine
29
30C:\tools\nssm\nssm.exe install KQ-TradeEngine "C:\Projects\klinequant\.venv\Scripts\python.exe" "-m core.trade_engine.engine"
31C:\tools\nssm\nssm.exe set KQ-TradeEngine AppDirectory "C:\Projects\klinequant"
32C:\tools\nssm\nssm.exe start KQ-TradeEngine
33
34C:\tools\nssm\nssm.exe install KQ-Gateway "C:\Projects\klinequant\.venv\Scripts\python.exe" "-m gateway.app"
35C:\tools\nssm\nssm.exe set KQ-Gateway AppDirectory "C:\Projects\klinequant"
36C:\tools\nssm\nssm.exe start KQ-Gateway
```

### 4.3 NSSM 服务管理命令


```plain
1# 查看所有 KQ 服务状态
2Get-Service KQ-* | Format-Table Name, Status, StartType
3
4# 启动/停止/重启
5C:\tools\nssm\nssm.exe start KQ-ClickHouse
6C:\tools\nssm\nssm.exe stop KQ-ClickHouse
7C:\tools\nssm\nssm.exe restart KQ-ClickHouse
8
9# 设置自动重启（崩溃后 5 秒重启）
10C:\tools\nssm\nssm.exe set KQ-MarketEngine AppRestartDelay 5000
11
12# 设置开机自启
13C:\tools\nssm\nssm.exe set KQ-ClickHouse Start SERVICE_AUTO_START
14
15# 卸载服务
16C:\tools\nssm\nssm.exe remove KQ-ClickHouse confirm
```

### 4.4 PowerShell 启动脚本（开发环境）
开发阶段使用脚本更灵活，无需注册服务：



```plain
1# scripts/start_all.ps1
2$ErrorActionPreference = "Stop"
3$ProjectRoot = "C:\Projects\klinequant"
4$Python = "$ProjectRoot\.venv\Scripts\python.exe"
5
6Write-Host "=== Starting KlineQuant Engines ===" -ForegroundColor Cyan
7
8# 1. 启动 ClickHouse（如果未作为服务运行）
9$chRunning = Get-Process -Name "clickhouse" -ErrorAction SilentlyContinue
10if (-not $chRunning) {
11    Write-Host "Starting ClickHouse..." -ForegroundColor Yellow
12    Start-Process -FilePath "C:\clickhouse\clickhouse.exe" `
13        -ArgumentList "server --config-file=C:\clickhouse\config.xml" `
14        -WorkingDirectory "C:\clickhouse" `
15        -WindowStyle Hidden
16    Start-Sleep -Seconds 3
17}
18
19# 2. 启动各引擎（新窗口）
20Start-Process -FilePath $Python -ArgumentList "-m core.market_engine.engine" -WorkingDirectory $ProjectRoot -WindowStyle Normal
21Start-Sleep -Seconds 2
22
23Start-Process -FilePath $Python -ArgumentList "-m core.indicator_engine.engine" -WorkingDirectory $ProjectRoot -WindowStyle Normal
24Start-Sleep -Seconds 1
25
26Start-Process -FilePath $Python -ArgumentList "-m core.signal_engine.engine" -WorkingDirectory $ProjectRoot -WindowStyle Normal
27Start-Sleep -Seconds 1
28
29Start-Process -FilePath $Python -ArgumentList "-m core.trade_engine.engine" -WorkingDirectory $ProjectRoot -WindowStyle Normal
30Start-Sleep -Seconds 1
31
32Start-Process -FilePath $Python -ArgumentList "-m gateway.app" -WorkingDirectory $ProjectRoot -WindowStyle Normal
33
34Write-Host "=== All engines started ===" -ForegroundColor Green
```



```plain
1# scripts/stop_all.ps1
2Write-Host "Stopping KlineQuant processes..." -ForegroundColor Yellow
3
4# 停止 Python 引擎
5Get-Process python -ErrorAction SilentlyContinue | Where-Object {
6    $_.CommandLine -like "*core.*engine*" -or $_.CommandLine -like "*gateway.app*"
7} | Stop-Process -Force
8
9# 停止 ClickHouse（如果由脚本启动）
10Get-Process clickhouse -ErrorAction SilentlyContinue | Stop-Process -Force
11
12Write-Host "All KlineQuant processes stopped." -ForegroundColor Green
```

---

## 5. 项目初始化（Windows 适配）
### 5.1 目录结构


```plain
1klinequant/
2├── data/                          # 数据目录
3│   ├── klinequant.duckdb          # DuckDB 主数据库
4│   ├── backtest/                  # 回测结果
5│   └── exports/                   # Parquet 导出
6├── .qoder/
7│   ├── rules/
8│   └── wiki/
9├── core/                          # 业务引擎
10├── strategy/                      # 策略模块
11├── gateway/                       # API 网关
12├── storage/                       # 存储层
13│   ├── __init__.py
14│   ├── duckdb_manager.py
15│   ├── duckdb_schema.py
16│   ├── clickhouse_manager.py
17│   ├── clickhouse_schema.py
18│   ├── clickhouse_writer.py
19│   ├── cache.py                   # 进程内缓存（替代 Redis）
20│   ├── analytics_bridge.py        # 跨库分析
21│   └── repositories/
22│       ├── __init__.py
23│       ├── kline_repo.py          # → ClickHouse
24│       ├── tick_repo.py           # → ClickHouse
25│       ├── order_repo.py          # → DuckDB
26│       ├── fill_repo.py           # → DuckDB
27│       ├── strategy_repo.py       # → DuckDB
28│       └── risk_log_repo.py       # → DuckDB
29├── protocol/                      # 协议定义
30├── frontend/                      # 前端
31├── config/
32│   ├── settings.yaml
33│   └── ...
34├── scripts/
35│   ├── start_all.ps1              # PowerShell 启动脚本
36│   ├── stop_all.ps1
37│   ├── init_clickhouse.ps1        # ClickHouse 初始化
38│   ├── init_duckdb.py             # DuckDB 初始化
39│   ├── health_check.ps1
40│   └── backup.ps1
41├── tests/
42├── .env.example
43├── .gitignore                     # Windows 适配
44├── .gitattributes
45├── justfile                       # 替代 Makefile（Windows 友好）
46├── pyproject.toml
47└── README.md
```

### 5.2 配置文件


```plain
1# config/settings.yaml
2system:
3  name: "KlineQuant"
4  mode: "paper"
5  log_level: "INFO"
6  timezone: "UTC"
7  data_dir: "./data"              # 数据根目录
8
9gateway:
10  host: "127.0.0.1"              # Windows 开发绑定 localhost
11  port: 8000
12  ws_port: 8001
13  jwt_secret: "${KQ_JWT_SECRET}"
14
15storage:
16  duckdb:
17    path: "./data/klinequant.duckdb"
18    # 无需 host/port/user/password（嵌入式）
19  clickhouse:
20    host: "localhost"             # Windows 原生 ClickHouse 监听 localhost
21    port: 8123                    # HTTP 接口
22    tcp_port: 9000                # 原生 TCP（clickhouse-connect 使用）
23    database: "klinequant"
24    username: "default"
25    password: ""                  # 开发环境无密码
26    # 写入配置
27    batch_size: 1000
28    flush_interval_sec: 2.0
29  cache:
30    # 进程内缓存配置（替代 Redis）
31    kline_maxsize: 5000
32    kline_ttl_sec: 60
33    ticker_ttl_sec: 5
34    account_ttl_sec: 10
35
36transport:
37  type: "zmq"
38  zmq:
39    host: "127.0.0.1"            # Windows 绑定 127.0.0.1
40    pub_base_port: 5501
41    rep_base_port: 5510
42
43market_engine:
44  exchanges:
45    - name: binance
46      ws_url: "wss://stream.binance.com:9443"
47      rest_url: "https://api.binance.com"
48      symbols: ["BTC-USDT", "ETH-USDT"]
49      timeframes: ["1m", "5m", "15m", "1h", "4h", "1d"]
50
51risk:
52  rules:
53    - id: "RISK-001"
54      enabled: true
55      params:
56        max_order_amount: 10000
57    - id: "RISK-004"
58      enabled: true
59      params:
60        max_daily_loss_pct: 0.05
```

### 5.3 .gitignore（Windows 适配）


```plain
1# Python
2__pycache__/
3*.py[cod]
4.venv/
5*.egg-info/
6dist/
7
8# DuckDB
9data/*.duckdb
10data/*.duckdb.wal
11data/backtest/
12data/exports/
13
14# ClickHouse（本地数据）
15clickhouse-data/
16
17# IDE
18.vscode/
19.idea/
20*.swp
21
22# Windows
23Thumbs.db
24Desktop.ini
25*.lnk
26
27# Environment
28.env
29.env.local
30
31# Node
32frontend/node_modules/
33frontend/dist/
34
35# Qoder
36.qoder/cache/
37
38# Logs
39logs/
40*.log
```

### 5.4 Justfile（替代 Makefile，Windows 原生支持）
安装：`winget install casey.just` 或 `cargo install just`



```plain
1# justfile — KlineQuant 开发命令
2
3set shell := ["powershell", "-Command"]
4
5# 默认命令
6default:
7    @just --list
8
9# === 环境 ===
10init:
11    uv venv .venv --python 3.11
12    .venv\Scripts\activate
13    uv sync --dev
14    python scripts/init_duckdb.py
15    powershell scripts/init_clickhouse.ps1
16
17# === 开发 ===
18dev-infra:
19    C:\clickhouse\clickhouse.exe server --config-file=C:\clickhouse\config.xml
20
21dev-server:
22    uvicorn gateway.app:create_app --factory --reload --port 8000
23
24dev-frontend:
25    cd frontend; pnpm dev
26
27dev-all:
28    powershell scripts/start_all.ps1
29
30stop-all:
31    powershell scripts/stop_all.ps1
32
33# === 代码质量 ===
34lint:
35    ruff check .
36    mypy core/ protocol/ gateway/ storage/
37
38format:
39    ruff format .
40    ruff check --fix .
41
42# === 测试 ===
43test:
44    pytest tests/unit/ -v --cov=core --cov=protocol --cov-report=term-missing
45
46test-integration:
47    pytest tests/integration/ -v
48
49test-all:
50    pytest tests/ -v --cov --cov-report=html
51
52# === 数据库 ===
53ch-init:
54    powershell scripts/init_clickhouse.ps1
55
56ch-client:
57    C:\clickhouse\clickhouse.exe client --database klinequant
58
59duck-shell:
60    python -c "import duckdb; conn = duckdb.connect('data/klinequant.duckdb'); conn.sql('SHOW TABLES').show(); input()"
```

---

## 6. Qoder Rules 配置（更新）
### 6.1 更新全局规则 — `.qoder/rules/global.md`


```plain
1## 平台与环境
2- 运行平台：Windows 10/11 (x64)，纯原生运行，无容器/WSL 依赖
3- Python 虚拟环境路径：.venv\Scripts\（非 bin/）
4- 路径分隔符：代码中使用 pathlib.Path，禁止硬编码 "/" 或 "\\"
5- 终端：PowerShell 7+（非 cmd）
6- 换行符：LF（.gitattributes 强制）
7- ClickHouse：Windows 原生二进制（C:\clickhouse\clickhouse.exe）
8
9## 数据库规则
10- 时序数据（K线/Tick/指标值）→ ClickHouse
11- 结构化数据（订单/策略/配置/日志）→ DuckDB
12- 缓存 → 进程内 cachetools（禁止引入 Redis）
13- DuckDB 写操作必须通过 DuckDBManager.execute_write（串行化）
14- ClickHouse 写入必须批量（≥ 100 条或 2s 超时）
15- 禁止在 async 函数中直接调用 duckdb/clickhouse 同步方法，必须 run_in_executor
16- 价格字段：Python 用 Decimal，ClickHouse 用 Decimal64(8)，DuckDB 用 DECIMAL(20,8)
17
18## Windows 特定
19- asyncio 事件循环：使用 WindowsProactorEventLoop（Python 3.11 默认）
20- ZMQ：绑定地址使用 "tcp://127.0.0.1:port"（非 "tcp://*:port"）
21- 文件路径：使用 Path 对象，配置文件中用正斜杠
22- 进程管理：使用 multiprocessing 的 "spawn" 模式（Windows 默认）
23- 信号处理：Windows 不支持 SIGTERM，使用 SIGBREAK 或条件变量
24- 服务管理：使用 NSSM 注册 Windows 服务，或 PowerShell 脚本管理
```

### 6.2 新增存储规则 — `.qoder/rules/storage.md`
**触发方式**：File-Specific（`storage/**`）



```plain
1# 存储层编码规范
2
3## DuckDB 规则
4- 连接通过 DuckDBManager 单例管理
5- 写操作（INSERT/UPDATE/DELETE）必须使用 execute_write（带锁）
6- 读操作使用 execute_read 或 query_df
7- 同步 API 通过 asyncio.run_in_executor 异步化
8- 禁止在多个进程中同时写入同一 .duckdb 文件
9- 回测结果写入独立 .duckdb 文件（避免锁竞争）
10- 大结果集使用 query_df 返回 DataFrame
11
12## ClickHouse 规则
13- 连接通过 ClickHouseManager 单例管理
14- 写入必须批量（使用 ClickHouseWriter 缓冲）
15- 单条 INSERT 是反模式，禁止
16- 查询使用参数化（防 SQL 注入）：{param:Type} 语法
17- LowCardinality(String) 用于低基数字段（symbol, exchange, timeframe）
18- 时间字段使用 DateTime64(3, 'UTC')
19- 分区键包含时间维度（按月/按天）
20- TTL 策略：K线 5年，Tick 90天，指标 1年
21
22## 缓存规则
23- 使用 cachetools.TTLCache / LRUCache
24- 缓存 key 格式："{type}:{symbol}:{timeframe}"
25- 禁止缓存可变对象（返回副本）
26- 缓存失效：数据更新时主动清除对应 key
27
28## Repository 模式
29- 每个数据实体一个 Repository 类
30- Repository 只依赖 Manager（不直接 import duckdb/clickhouse）
31- 返回领域对象（protocol.types），不返回原始 row
32- 批量操作提供 xxx_batch 方法
```

---

## 7. 存储层开发指南
### 7.1 存储层架构图


```plain
1┌─────────────────────────────────────────────────────────────┐
2│                      业务引擎层                               │
3│  MarketEngine │ TradeEngine │ RiskEngine │ BacktestEngine    │
4└───────┬───────┴──────┬──────┴─────┬──────┴────────┬─────────┘
5        │              │            │               │
6        ▼              ▼            ▼               ▼
7┌─────────────────────────────────────────────────────────────┐
8│                    Repository 层                              │
9│  KlineRepo(CH) │ OrderRepo(Duck) │ RiskLogRepo(Duck) │ ...  │
10└───────┬────────┴──────┬───────────┴────────┬────────────────┘
11        │               │                    │
12        ▼               ▼                    ▼
13┌──────────────┐ ┌──────────────┐  ┌──────────────────┐
14│ ClickHouse   │ │   DuckDB     │  │  CacheManager    │
15│ Manager      │ │   Manager    │  │  (cachetools)    │
16└──────┬───────┘ └──────┬───────┘  └──────────────────┘
17       │                │
18       ▼                ▼
19┌──────────────┐ ┌──────────────────┐
20│ ClickHouse   │ │ data/klinequant  │
21│ Server       │ │ .duckdb          │
22│ (Windows     │ │ (嵌入式文件)     │
23│  原生服务)   │ │                  │
24└──────────────┘ └──────────────────┘
```

### 7.2 Quest Spec：存储层实现


```plain
1## 任务：实现存储层（DuckDB + ClickHouse + 进程内缓存）
2
3### 目标
4实现 `storage/` 模块，提供统一的数据持久化和缓存能力。
5
6### 需要创建的文件
7
8#### 1. `storage/duckdb_manager.py`
9- DuckDBManager 类（单例）
10- 异步包装（run_in_executor）
11- 写锁（asyncio.Lock）
12- initialize() 执行 Schema
13- execute_write / execute_read / query_df
14- close()
15
16#### 2. `storage/duckdb_schema.py`
17- DUCKDB_SCHEMA 字符串常量
18- 包含：orders, fills, strategies, risk_logs, backtest_results, audit_logs, sys_config
19- 所有金额 DECIMAL(20,8)
20- 索引定义
21
22#### 3. `storage/clickhouse_manager.py`
23- ClickHouseManager 类
24- 使用 clickhouse-connect 驱动
25- 异步包装
26- query / query_df / insert / execute
27- 连接池（clickhouse-connect 内置）
28
29#### 4. `storage/clickhouse_schema.py`
30- CLICKHOUSE_SCHEMA 字符串常量
31- 包含：klines, ticks, indicator_values, market_snapshot
32- MergeTree 引擎，合理分区和排序键
33- TTL 策略
34
35#### 5. `storage/clickhouse_writer.py`
36- ClickHouseWriter 类
37- 缓冲 + 批量写入
38- 按表分缓冲
39- 定时刷新（2s）+ 阈值刷新（1000条）
40- 优雅关闭时 flush 剩余
41
42#### 6. `storage/cache.py`
43- CacheManager 类
44- 基于 cachetools
45- kline_cache / ticker_cache / account_cache / symbol_cache
46- get / set / invalidate 方法
47- 线程安全
48
49#### 7. `storage/repositories/kline_repo.py`
50- KlineRepository（ClickHouse）
51- save_batch / get_klines / get_latest / get_range
52
53#### 8. `storage/repositories/order_repo.py`
54- OrderRepository（DuckDB）
55- save / get_by_id / get_open_orders / update_status / get_history
56
57#### 9. `storage/repositories/strategy_repo.py`
58- StrategyRepository（DuckDB）
59- CRUD + 状态更新
60
61#### 10. `storage/repositories/risk_log_repo.py`
62- RiskLogRepository（DuckDB）
63- save / query_by_time_range / query_by_strategy
64
65#### 11. `storage/analytics_bridge.py`
66- AnalyticsBridge
67- ClickHouse → Parquet → DuckDB 分析
68
69### 约束
70- 所有 I/O 异步化（run_in_executor）
71- DuckDB 写操作串行化
72- ClickHouse 写入批量化
73- 不引入 Redis / PostgreSQL / Docker / WSL 任何依赖
74- Windows 路径兼容（pathlib）
75- 编写单元测试（DuckDB 用 :memory:，ClickHouse 用 mock）
76
77### 验收标准
78- [ ] DuckDB Schema 正确创建
79- [ ] ClickHouse 表正确创建
80- [ ] K线批量写入 ClickHouse 正常
81- [ ] 订单 CRUD（DuckDB）正常
82- [ ] 缓存 TTL 过期生效
83- [ ] 跨库查询（Parquet 桥接）正常
84- [ ] 单元测试通过
85- [ ] Windows 路径无问题
```

---

## 8. Windows 平台适配要点
### 8.1 asyncio 事件循环


```plain
1# main.py — 入口文件
2import asyncio
3import sys
4
5def main():
6    # Windows 上 Python 3.11 默认使用 ProactorEventLoop
7    # ZMQ 需要 SelectorEventLoop（或 pyzmq 已兼容）
8    if sys.platform == "win32":
9        # pyzmq >= 25 已兼容 ProactorEventLoop，无需切换
10        # 如遇到问题，取消下行注释：
11        # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
12        pass
13
14    asyncio.run(run_app())
15
16async def run_app():
17    # 启动所有引擎...
18    ...
19
20if __name__ == "__main__":
21    main()
```

### 8.2 路径处理


```plain
1# ✅ 正确：使用 pathlib
2from pathlib import Path
3
4DATA_DIR = Path("data")
5DB_PATH = DATA_DIR / "klinequant.duckdb"
6CONFIG_PATH = Path("config") / "settings.yaml"
7
8# 读取配置
9import yaml
10with open(CONFIG_PATH, "r", encoding="utf-8") as f:
11    config = yaml.safe_load(f)
12
13# ❌ 错误：硬编码分隔符
14# db_path = "data\\klinequant.duckdb"
15# db_path = "data/klinequant.duckdb"  # 虽然 Python 支持，但不规范
```

### 8.3 进程管理（策略沙箱）


```plain
1# strategy/sandbox/runner.py
2"""策略进程管理器（Windows 适配）。"""
3import multiprocessing as mp
4import sys
5from pathlib import Path
6
7# Windows 必须使用 spawn（不支持 fork）
8if sys.platform == "win32":
9    mp.set_start_method("spawn", force=True)
10
11
12class StrategyRunner:
13    """管理策略子进程。"""
14
15    MAX_RESTARTS = 3
16    RESTART_WINDOW_SEC = 300  # 5 分钟
17
18    def __init__(self):
19        self._processes: dict[str, mp.Process] = {}
20        self._restart_counts: dict[str, list[float]] = {}
21
22    def start_strategy(self, strategy_id: str, module_path: str, params: dict) -> None:
23        """启动策略进程。"""
24        if strategy_id in self._processes:
25            raise RuntimeError(f"Strategy {strategy_id} already running")
26
27        proc = mp.Process(
28            target=self._run_strategy,
29            args=(strategy_id, module_path, params),
30            name=f"strategy-{strategy_id}",
31            daemon=True,  # 主进程退出时子进程也退出
32        )
33        proc.start()
34        self._processes[strategy_id] = proc
35
36    def _run_strategy(self, strategy_id: str, module_path: str, params: dict) -> None:
37        """子进程入口（在独立进程中执行）。"""
38        import asyncio
39        asyncio.run(self._strategy_main(strategy_id, module_path, params))
40
41    async def _strategy_main(self, strategy_id: str, module_path: str, params: dict):
42        # 加载策略模块、建立 ZMQ 连接、运行主循环...
43        ...
44
45    def stop_strategy(self, strategy_id: str) -> None:
46        """停止策略进程。"""
47        proc = self._processes.pop(strategy_id, None)
48        if proc and proc.is_alive():
49            proc.terminate()
50            proc.join(timeout=5)
51            if proc.is_alive():
52                proc.kill()  # Windows 上 terminate 即 kill
```

### 8.4 信号处理（Windows 限制）


```plain
1# Windows 不支持 SIGTERM / SIGUSR1 等 Unix 信号
2# 使用以下替代方案：
3
4import signal
5import asyncio
6
7class GracefulShutdown:
8    """Windows 兼容的优雅关闭。"""
9
10    def __init__(self):
11        self._shutdown_event = asyncio.Event()
12
13    def register(self):
14        # Windows 支持的信号：SIGINT, SIGBREAK
15        signal.signal(signal.SIGINT, self._handler)
16        if hasattr(signal, "SIGBREAK"):  # Windows 特有
17            signal.signal(signal.SIGBREAK, self._handler)
18        # SIGTERM 在 Windows 上有限支持
19        try:
20            signal.signal(signal.SIGTERM, self._handler)
21        except (OSError, ValueError):
22            pass
23
24    def _handler(self, signum, frame):
25        self._shutdown_event.set()
26
27    async def wait(self):
28        await self._shutdown_event.wait()
```

### 8.5 ZMQ Windows 注意事项


```plain
1# ZMQ 在 Windows 上的绑定地址
2# ✅ 使用 127.0.0.1（明确）
3PUB_ADDRESS = "tcp://127.0.0.1:5501"
4
5# ❌ 避免使用 *（Windows 防火墙可能拦截）
6# PUB_ADDRESS = "tcp://*:5501"
7
8# 如果 Windows 防火墙弹窗，允许 Python 通过专用网络
9# 或添加防火墙规则：
10# netsh advfirewall firewall add rule name="KQ-ZMQ" dir=in action=allow protocol=TCP localport=5501-5530
```

### 8.6 .gitattributes（强制 LF）


```plain
1# .gitattributes
2* text=auto eol=lf
3*.py text eol=lf
4*.yaml text eol=lf
5*.toml text eol=lf
6*.ps1 text eol=crlf
7*.bat text eol=crlf
```

---

## 9. 部署与运维（Windows）
### 9.1 开发环境启动流程


```plain
1# === 一键启动开发环境 ===
2
3# 1. 启动 ClickHouse（Windows 原生）
4# 如果已注册为服务：
5net start KQ-ClickHouse
6# 如果手动启动：
7Start-Process -FilePath "C:\clickhouse\clickhouse.exe" `
8    -ArgumentList "server --config-file=C:\clickhouse\config.xml" `
9    -WorkingDirectory "C:\clickhouse" -WindowStyle Hidden
10
11# 2. 激活 Python 环境
12cd C:\Projects\klinequant
13.venv\Scripts\activate
14
15# 3. 初始化数据库（首次）
16python scripts/init_duckdb.py
17python scripts/init_clickhouse.py
18
19# 4. 启动所有引擎
20powershell scripts/start_all.ps1
21
22# 5. 启动前端开发服务器
23cd frontend
24pnpm dev
25
26# 6. 打开浏览器
27start http://localhost:3000
```

### 9.2 健康检查脚本


```plain
1# scripts/health_check.ps1
2Write-Host "=== KlineQuant Health Check ===" -ForegroundColor Cyan
3
4# ClickHouse
5try {
6    $ch = Invoke-RestMethod -Uri "http://localhost:8123/?query=SELECT+1" -TimeoutSec 3
7    Write-Host "[OK] ClickHouse: responding" -ForegroundColor Green
8} catch {
9    Write-Host "[FAIL] ClickHouse: unreachable" -ForegroundColor Red
10}
11
12# DuckDB
13try {
14    python -c "import duckdb; c=duckdb.connect('data/klinequant.duckdb', read_only=True); c.execute('SELECT 1'); c.close()"
15    Write-Host "[OK] DuckDB: accessible" -ForegroundColor Green
16} catch {
17    Write-Host "[FAIL] DuckDB: error" -ForegroundColor Red
18}
19
20# Gateway
21try {
22    $gw = Invoke-RestMethod -Uri "http://localhost:8000/api/v1/system/health" -TimeoutSec 3
23    Write-Host "[OK] Gateway: $($gw.status)" -ForegroundColor Green
24} catch {
25    Write-Host "[FAIL] Gateway: unreachable" -ForegroundColor Red
26}
27
28# ZMQ ports
29$ports = 5501..5504 + 5510..5511
30foreach ($port in $ports) {
31    $conn = Test-NetConnection -ComputerName 127.0.0.1 -Port $port -WarningAction SilentlyContinue
32    if ($conn.TcpTestSucceeded) {
33        Write-Host "[OK] ZMQ port $port : listening" -ForegroundColor Green
34    } else {
35        Write-Host "[WARN] ZMQ port $port : not listening" -ForegroundColor Yellow
36    }
37}
38
39# Python processes
40$procs = Get-Process python -ErrorAction SilentlyContinue | Measure-Object
41Write-Host "[INFO] Python processes running: $($procs.Count)" -ForegroundColor White
42
43# Windows Services
44$services = Get-Service KQ-* -ErrorAction SilentlyContinue
45if ($services) {
46    Write-Host "`n=== Windows Services ===" -ForegroundColor Cyan
47    $services | Format-Table Name, Status, StartType -AutoSize
48}
```

### 9.3 日志管理（Windows）


```plain
1# 日志配置（loguru）
2from loguru import logger
3from pathlib import Path
4import sys
5
6LOG_DIR = Path("logs")
7LOG_DIR.mkdir(exist_ok=True)
8
9# 移除默认 handler
10logger.remove()
11
12# 控制台输出
13logger.add(
14    sys.stderr,
15    format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level:<7}</level> | <cyan>{extra[component]}</cyan> | {message}",
16    level="INFO",
17)
18
19# 文件输出（按天轮转，保留 30 天）
20logger.add(
21    LOG_DIR / "klinequant_{time:YYYY-MM-DD}.log",
22    rotation="00:00",       # 每天午夜轮转
23    retention="30 days",
24    compression="zip",      # Windows 支持 zip
25    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<7} | {extra[component]} | {message}",
26    level="DEBUG",
27    encoding="utf-8",
28)
29
30# 错误日志单独文件
31logger.add(
32    LOG_DIR / "error_{time:YYYY-MM-DD}.log",
33    rotation="00:00",
34    retention="90 days",
35    level="ERROR",
36    encoding="utf-8",
37)
```

### 9.4 数据备份（Windows 计划任务）


```plain
1# scripts/backup.ps1
2$BackupDir = "C:\Backups\klinequant\$(Get-Date -Format 'yyyyMMdd_HHmmss')"
3New-Item -ItemType Directory -Path $BackupDir -Force
4
5# DuckDB 备份（复制文件，需确保无写入）
6Copy-Item "C:\Projects\klinequant\data\klinequant.duckdb" "$BackupDir\"
7
8# ClickHouse 备份（Windows 原生客户端）
9& "C:\clickhouse\clickhouse.exe" client --query "BACKUP DATABASE klinequant TO Disk('backups', 'klinequant_$(Get-Date -Format yyyyMMdd).zip')"
10
11# 配置文件备份
12Copy-Item "C:\Projects\klinequant\config\*" "$BackupDir\config\" -Recurse
13
14Write-Host "Backup completed: $BackupDir"
```



```plain
1# 注册 Windows 计划任务（每日凌晨 3 点备份）
2$action = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-File C:\Projects\klinequant\scripts\backup.ps1"
3$trigger = New-ScheduledTaskTrigger -Daily -At "03:00"
4Register-ScheduledTask -TaskName "KQ-DailyBackup" -Action $action -Trigger $trigger -RunLevel Highest
```

---

## 10. Quest Spec 更新（存储相关）
### 10.1 原 Q3（存储层）替换为：
见第 7.2 节的完整 Quest Spec

### 10.2 Q4（行情引擎）追加存储相关：


```plain
1### 追加：行情数据持久化
2- K 线收盘后，通过 KlineRepository.save_batch 写入 ClickHouse
3- 使用 ClickHouseWriter 批量缓冲（非逐条写入）
4- 启动时从 ClickHouse 查询最近 K 线，检查是否有缺失
5- 缺失数据通过 REST API 补全后写入 ClickHouse
6- 最新 K 线快照写入 CacheManager（进程内缓存）
```

### 10.3 Q8（交易引擎）追加存储相关：


```plain
1### 追加：订单持久化
2- 订单创建时写入 DuckDB（OrderRepository.save）
3- 状态变更时更新 DuckDB（OrderRepository.update_status）
4- 成交记录写入 DuckDB（FillRepository.save）
5- 持仓快照定期写入 CacheManager（5s 刷新）
6- 每日结算数据写入 DuckDB
```

### 10.4 Q10（回测引擎）追加：


```plain
1### 追加：回测数据源
2- 历史 K 线从 ClickHouse 查询（KlineRepository.get_range）
3- 大数据量回测：先导出为 Parquet，再用 DuckDB 分析
4- 回测结果写入独立 DuckDB 文件（data/backtest/bt_{id}.duckdb）
5- 绩效指标写入主 DuckDB 的 backtest_results 表
6- 资金曲线数据以 JSON 存入 metrics 字段
```

---

## 11. 性能与调优
### 11.1 ClickHouse 调优（开发机）


```plain
1<!-- 开发环境推荐配置 -->
2<profiles>
3    <default>
4        <max_memory_usage>4000000000</max_memory_usage>     <!-- 4GB -->
5        <max_threads>4</max_threads>                         <!-- CPU 核心数 -->
6        <max_insert_block_size>1048576</max_insert_block_size>
7        <min_insert_block_size_rows>1000</min_insert_block_size_rows>
8    </default>
9</profiles>
10
11<merge_tree>
12    <max_bytes_to_merge_at_max_space_in_pool>1073741824</max_bytes_to_merge_at_max_space_in_pool>  <!-- 1GB -->
13    <number_of_free_entries_in_pool_to_lower_max_size_of_merge>5</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
14</merge_tree>
```

### 11.2 DuckDB 调优


```plain
1# 初始化时设置
2conn = duckdb.connect("data/klinequant.duckdb")
3conn.execute("SET threads = 4")              # CPU 核心数
4conn.execute("SET memory_limit = '2GB'")     # 内存限制
5conn.execute("SET temp_directory = 'data/tmp'")  # 溢出目录
```

### 11.3 写入性能基准


| **操作** | **目标** | **测量方式** |
| :--- | :--- | :--- |
| ClickHouse K线写入（批量 1000） | < 50ms | 计时 |
| DuckDB 订单写入（单条） | < 5ms | 计时 |
| DuckDB 订单查询（by ID） | < 1ms | 计时 |
| ClickHouse K线查询（500条） | < 100ms | 计时 |
| 缓存读取 | < 0.01ms | 计时 |


---

## 12. 迁移与备份
### 12.1 ClickHouse Schema 版本管理


```plain
1# scripts/init_clickhouse.py
2"""ClickHouse 初始化与迁移脚本。"""
3import clickhouse_connect
4from pathlib import Path
5
6MIGRATIONS_DIR = Path("storage/migrations/clickhouse")
7
8def get_applied_versions(client) -> set[int]:
9    """获取已执行的迁移版本。"""
10    client.command("""
11        CREATE TABLE IF NOT EXISTS klinequant.schema_migrations (
12            version UInt32,
13            applied_at DateTime DEFAULT now()
14        ) ENGINE = MergeTree() ORDER BY version
15    """)
16    result = client.query("SELECT version FROM klinequant.schema_migrations")
17    return {row[0] for row in result.result_rows}
18
19def apply_migrations(client):
20    """按顺序执行未应用的迁移。"""
21    applied = get_applied_versions(client)
22    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
23
24    for f in migration_files:
25        version = int(f.stem.split("_")[0])  # 001_create_klines.sql → 1
26        if version in applied:
27            continue
28        print(f"Applying migration: {f.name}")
29        sql = f.read_text(encoding="utf-8")
30        client.command(sql)
31        client.command(f"INSERT INTO klinequant.schema_migrations (version) VALUES ({version})")
32        print(f"  ✓ Applied")
33
34if __name__ == "__main__":
35    client = clickhouse_connect.get_client(host="localhost", port=8123)
36    apply_migrations(client)
37    print("All migrations applied.")
```

### 12.2 迁移文件示例


```plain
1-- storage/migrations/clickhouse/001_create_klines.sql
2CREATE DATABASE IF NOT EXISTS klinequant;
3
4CREATE TABLE IF NOT EXISTS klinequant.klines (
5    timestamp   DateTime64(3, 'UTC'),
6    symbol      LowCardinality(String),
7    exchange    LowCardinality(String),
8    timeframe   LowCardinality(String),
9    open        Decimal64(8),
10    high        Decimal64(8),
11    low         Decimal64(8),
12    close       Decimal64(8),
13    volume      Decimal64(8),
14    quote_volume Decimal64(8),
15    trade_count UInt32,
16    is_closed   UInt8
17)
18ENGINE = MergeTree()
19PARTITION BY (exchange, symbol, toYYYYMM(timestamp))
20ORDER BY (symbol, exchange, timeframe, timestamp)
21TTL timestamp + INTERVAL 5 YEAR DELETE;
```



```plain
1-- storage/migrations/clickhouse/002_create_ticks.sql
2CREATE TABLE IF NOT EXISTS klinequant.ticks (
3    ...
4) ENGINE = MergeTree()
5PARTITION BY (symbol, toYYYYMMDD(timestamp))
6ORDER BY (symbol, timestamp)
7TTL timestamp + INTERVAL 90 DAY DELETE;
```

### 12.3 DuckDB 无需迁移工具
DuckDB Schema 变更直接在 `duckdb_schema.py` 中维护，使用 `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN IF NOT EXISTS` 实现幂等：



```plain
1# storage/duckdb_schema.py 追加
2DUCKDB_MIGRATIONS = [
3    # v2: 订单表增加 leverage 字段
4    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS leverage INTEGER DEFAULT 1",
5    # v3: 增加索引
6    "CREATE INDEX IF NOT EXISTS idx_orders_updated ON orders(updated_at)",
7]
8
9async def apply_duckdb_migrations(conn):
10    for sql in DUCKDB_MIGRATIONS:
11        conn.execute(sql)
```

---

## 附录 A：环境变量（Windows）


```plain
1# .env.example（PowerShell 格式说明）
2# 实际使用 .env 文件 + pydantic-settings 加载
3
4# === ClickHouse ===
5KQ_CH_HOST=localhost
6KQ_CH_PORT=8123
7KQ_CH_DATABASE=klinequant
8KQ_CH_USER=default
9KQ_CH_PASSWORD=
10
11# === DuckDB ===
12KQ_DUCKDB_PATH=./data/klinequant.duckdb
13
14# === 交易所 ===
15KQ_BINANCE_API_KEY=your_key
16KQ_BINANCE_API_SECRET=your_secret
17
18# === 认证 ===
19KQ_JWT_SECRET=change_me_in_production
20KQ_ADMIN_PASSWORD=initial_password
21
22# === 系统 ===
23KQ_MODE=paper
24KQ_LOG_LEVEL=INFO
```

---

## 附录 B：Windows 防火墙规则


```plain
1# 以管理员运行（仅开发环境需要，如果 ZMQ 绑定 127.0.0.1 则不需要）
2netsh advfirewall firewall add rule name="KQ-Gateway" dir=in action=allow protocol=TCP localport=8000-8001
3netsh advfirewall firewall add rule name="KQ-ClickHouse" dir=in action=allow protocol=TCP localport=8123,9000
4netsh advfirewall firewall add rule name="KQ-ZMQ" dir=in action=allow protocol=TCP localport=5501-5530
```

---

## 附录 C：常见问题


| **问题** | **原因** | **解决方案** |
| :--- | :--- | :--- |
| `zmq.error.ZMQError: Address in use` | 上次进程未正常退出 | `netstat -ano | findstr :5501`<br/> → `taskkill /PID xxx /F` |
| ClickHouse 连接拒绝 | 服务未启动 | `net start KQ-ClickHouse`<br/> 或手动运行 `clickhouse.exe server` |
| `DuckDB IOError: Could not set lock` | 另一进程占用 | 确保只有一个进程写入，回测用独立文件 |
| `asyncio.EventLoop is closed` | Windows Proactor 清理顺序 | 在 `run_app()`<br/> 末尾加 `await asyncio.sleep(0.1)` |
| Python 找不到 `.venv` | 路径中有空格 | 使用引号：`& "C:\My Projects\kq\.venv\Scripts\python.exe"` |
| ClickHouse 中文乱码 | 编码问题 | 确保 `encoding="utf-8"`<br/> everywhere |
| `polars` 大批量指标计算内存峰值 | 多品种×多周期同时预热 | 分批预热（每批 10 个品种），使用 `pl.collect_all()` 共享线程池 |
| NSSM 服务启动失败 | 路径或参数错误 | 使用 `nssm edit KQ-xxx`<br/> 打开 GUI 检查配置 |
| ClickHouse 内存占用过高 | 默认配置过大 | 修改 `config.xml`<br/> 中 `max_memory_usage` |
| 端口被占用 | 其他程序占用 | `netstat -ano | findstr :8123`<br/> 查找并关闭 |


---

## 附录 D：ClickHouse Windows 服务自动恢复配置


```plain
1# 配置 ClickHouse 服务崩溃后自动重启
2C:\tools\nssm\nssm.exe set KQ-ClickHouse AppExit Default Restart
3C:\tools\nssm\nssm.exe set KQ-ClickHouse AppRestartDelay 5000
4
5# 配置所有引擎服务崩溃后自动重启
6$services = @("KQ-MarketEngine", "KQ-IndicatorEngine", "KQ-SignalEngine", "KQ-TradeEngine", "KQ-Gateway")
7foreach ($svc in $services) {
8    C:\tools\nssm\nssm.exe set $svc AppExit Default Restart
9    C:\tools\nssm\nssm.exe set $svc AppRestartDelay 3000
10}
11
12# 验证服务配置
13Get-Service KQ-* | ForEach-Object {
14    Write-Host "$($_.Name): $($_.Status) (StartType: $($_.StartType))" -ForegroundColor Cyan
15}
```

---

**文档结束**<font style="color:rgb(34, 34, 34);">  
</font>本文档为 KlineQuant 在 **纯 Windows 平台**（无容器、无 WSL）使用 DuckDB + ClickHouse 的完整技术开发指南。<font style="color:rgb(34, 34, 34);">  
</font>所有 Qoder Quest Spec 可直接复制到 Qoder Desktop 的 Quest Mode 中执行。<font style="color:rgb(34, 34, 34);">  
</font>建议将本文档存入 `docs/development-guide-win-native.md`。

---

### 
