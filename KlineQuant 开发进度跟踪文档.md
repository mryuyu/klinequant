# KlineQuant 开发进度跟踪文档

> **版本**：v6.0  
> **创建日期**：2026-07-29  
> **最后更新**：2026-07-30  
> **状态标记**：⬜ 未开始 / 🔄 进行中 / ✅ 已完成 / ❌ 失败 / ⏸️ 暂停  
> **测试标记**：🧪 待测试 / ✅ 测试通过 / ❌ 测试失败  
> **交付原则**：每个模块必须单元测试通过后方可交付

---

## 一、环境基线

| 组件 | 状态 | 版本/备注 |
|------|------|----------|
| Python | ✅ | 3.12.1（满足 3.10+ 要求） |
| pip | ✅ | 25.0.1 |
| git | ✅ | 2.45.1 |
| uv | ✅ | 0.12.0 |
| Redis | ✅ | 运行中（端口 6379） |
| ClickHouse | ⏸️ | 无 Windows 原生客户端，Phase 1 用 DuckDB 替代 |
| Node.js | ✅ | v22.16.0 + npm 10.9.2 |

---

## 二、Phase 1：核心闭环（W1-10）

### 2.1 基础设施搭建（W1-2）

#### 1.1 项目骨架初始化

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| B-001 | 创建项目目录结构 | P0 | ✅ | — | — | 技术文档 §5.1，34个目录+26个__init__.py |
| B-002 | 初始化 pyproject.toml | P0 | ✅ | — | B-001 | uv 管理依赖，requires-python >=3.10 |
| B-003 | 创建虚拟环境并安装依赖 | P0 | ✅ | — | B-002 | Python 3.12.1，63+16包已安装 |
| B-004 | 配置 .gitignore（Windows 适配） | P0 | ✅ | — | B-001 | — |
| B-005 | 配置 .gitattributes（强制 LF） | P0 | ✅ | — | B-001 | — |
| B-006 | 创建 .env.example | P1 | ✅ | — | B-001 | — |
| B-007 | 配置 ruff + mypy 代码规范 | P1 | ✅ | — | B-002 | pyproject.toml 中已配置 |

#### 1.2 核心数据结构（protocol 包）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| P-001 | Kline dataclass（标准化 K 线） | P0 | ✅ | ✅ | B-001 | 含约束校验 |
| P-002 | Tick dataclass（逐笔/盘口） | P0 | ✅ | ✅ | B-001 | frozen dataclass |
| P-003 | Order + OrderSide/Type/Status + 状态流转 | P0 | ✅ | ✅ | B-001 | 含合法状态流转表 |
| P-004 | Position dataclass | P0 | ✅ | ✅ | P-003 | — |
| P-005 | Signal + SignalDirection/Strength | P0 | ✅ | ✅ | P-001 | 含 is_expired |
| P-006 | Account dataclass | P0 | ✅ | ✅ | P-004 | — |
| P-007 | IndicatorValue dataclass | P0 | ✅ | ✅ | P-001 | 嵌套 dict |
| P-008 | SymbolInfo dataclass | P0 | ✅ | ✅ | B-001 | 含精度校验 |
| P-009 | Message 消息信封 | P0 | ✅ | ✅ | B-001 | +MessageType注册表+路由校验 |
| P-010 | msgpack 序列化/反序列化（codec.py） | P0 | ✅ | ✅ | P-009 | Decimal/Enum扩展编码 |

**P 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| P-T-001 | Kline 字段类型校验、约束校验（high >= max(open,close)） | ✅ |
| P-T-002 | Tick 字段类型校验 | ✅ |
| P-T-003 | Order 状态流转（合法/非法转换） | ✅ |
| P-T-004 | Position 字段校验、盈亏计算 | ✅ |
| P-T-005 | Signal 字段校验、expires_at 过期判断 | ✅ |
| P-T-006 | Account 余额字段非负校验 | ✅ |
| P-T-007 | IndicatorValue 嵌套 dict 结构 | ✅ |
| P-T-008 | SymbolInfo 精度字段校验 | ✅ |
| P-T-009 | Message 序列化/反序列化往返一致性 | ✅ |
| P-T-010 | codec：Kline/Order/Signal 等类型的序列化兼容性 | ✅ |

#### 1.3 通信层（Transport）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| T-001 | Transport 抽象基类 | P0 | ✅ | ✅ | P-009 | 需求文档 §7.3 |
| T-002 | ZmqTransport（PUB/SUB 广播） | P0 | ✅ | ✅ | T-001 | ZMQ 绑定 127.0.0.1，含通配符订阅 |
| T-003 | ZmqTransport（REQ/REP 请求响应） | P0 | ✅ | ✅ | T-001 | DEALER+REP 架构，避免 REQ 状态机问题 |
| T-004 | 消息类型注册表 | P0 | ✅ | ✅ | P-009 | 复用 protocol 层，含路由校验 |
| T-005 | ZMQ 端口规划验证（5501-5530） | P1 | ✅ | ✅ | T-002 | PortRegistry 类，含策略沙箱端口分配 |

**T 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| T-T-001 | Transport 抽象接口完整性 | ✅ |
| T-T-002 | ZMQ PUB/SUB 发布-接收往返（多主题/通配符/取消订阅） | ✅ |
| T-T-003 | ZMQ REQ/REP 请求-响应往返（多次请求/超时） | ✅ |
| T-T-004 | 端口注册表：主题/服务映射 + 策略端口分配/耗尽检测 | ✅ |
| T-T-005 | ZmqTransport 角色参数校验 + 集成测试 | ✅ |

#### 1.4 数据库基础设施（DuckDB 统一存储 + Redis 缓存）

> **架构决策**：ClickHouse 无 Windows 原生客户端，Phase 1 闭环阶段统一使用 DuckDB 存储全部数据（时序 + 结构化），远期可迁移至 ClickHouse 做时序层。

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| DB-001 | DuckDBManager（单例 + 写锁 + 异步封装） | P0 | ✅ | ✅ | B-001 | 核心连接管理器 |
| DB-002 | DuckDB Schema：klines 表（时序数据） | P0 | ✅ | ✅ | DB-001 | 替代原 ClickHouse klines |
| DB-003 | DuckDB Schema：ticks 表 | P1 | ✅ | ✅ | DB-001 | — |
| DB-004 | DuckDB Schema：indicator_values 表 | P0 | ✅ | ✅ | DB-001 | — |
| DB-005 | DuckDB Schema：orders/fills/strategies/risk_logs 表 | P0 | ✅ | ✅ | DB-001 | 结构化业务数据 |
| DB-006 | DuckDB Schema：backtest_results/audit_logs/sys_config 表 | P1 | ✅ | ✅ | DB-001 | — |
| DB-007 | DuckDB 批量写入缓冲（BatchWriter） | P0 | ✅ | ✅ | DB-001 | K线/Tick/指标高吞吐写入 |
| DB-008 | RedisCacheManager（Redis 连接 + TTL 策略） | P0 | ✅ | ✅ | B-001 | KV/Hash/mget/mset/delete_pattern |

**DB 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| DB-T-001 | DuckDBManager 单例 + 写锁互斥 + CRUD 接口 | ✅ |
| DB-T-002 | DuckDB Schema 迁移幂等性 + 10 张表全部创建 | ✅ |
| DB-T-003 | klines 表插入/查询/时间范围/closed 过滤/主键替换 | ✅ |
| DB-T-004 | BatchWriter 缓冲/自动刷新/批量/stop刷新/工厂函数 | ✅ |
| DB-T-005 | Redis KV/Hash/TTL/mget_mset/Decimal/pattern 删除 | ✅ |

#### 1.5 存储层（Repository）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| R-001 | KlineRepository（DuckDB CRUD：save_batch/get_klines/get_latest/get_range） | P0 | ✅ | ✅ | DB-002 | — |
| R-002 | OrderRepository（DuckDB CRUD） | P0 | ✅ | ✅ | DB-005 | save/get_by_id/update_status/get_open_orders |
| R-003 | FillRepository（DuckDB） | P0 | ✅ | ✅ | DB-005 | save/query_by_order/query_by_strategy |
| R-004 | StrategyRepository（DuckDB） | P0 | ✅ | ✅ | DB-005 | CRUD + update_status/update_config |
| R-005 | RiskLogRepository（DuckDB） | P0 | ✅ | ✅ | DB-005 | save/query_by_time_range/query_by_level |
| R-006 | TickRepository（DuckDB） | P1 | ✅ | ✅ | DB-003 | save_batch/query_latest/query_range |

**R 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| R-T-001 | KlineRepository save_batch / get_klines / get_latest / upsert | ✅ |
| R-T-002 | OrderRepository save / get_by_id / update_status / get_open | ✅ |
| R-T-003 | FillRepository save / query_by_order / query_by_strategy | ✅ |
| R-T-004 | StrategyRepository CRUD + 状态更新 + 配置更新 | ✅ |
| R-T-005 | RiskLogRepository save / query_by_time_range / query_by_level | ✅ |

#### 1.6 日志与配置

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| C-001 | loguru 日志配置（控制台 + 文件轮转） | P0 | ✅ | ✅ | B-001 | — |
| C-002 | config/settings.yaml（Windows 适配） | P0 | ✅ | — | B-001 | — |
| C-003 | config/exchanges.yaml | P0 | ✅ | — | B-001 | — |
| C-004 | pydantic-settings 配置加载 | P0 | ✅ | ✅ | C-002 | — |
| C-005 | GracefulShutdown（SIGINT/SIGBREAK） | P0 | ✅ | ✅ | B-001 | Windows 适配 |

---

### 2.2 行情引擎 MarketEngine（W3-4）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| MKT-001 | ExchangeAdapter 抽象基类 | P0 | ✅ | ✅ | P-001,P-008 | — |
| MKT-002 | BinanceAdapter：WS 实时行情 | P0 | ✅ | ✅ | MKT-001 | — |
| MKT-003 | BinanceAdapter：REST 历史 K 线 | P0 | ✅ | ✅ | MKT-002 | — |
| MKT-004 | K 线标准化（normalizer.py） | P0 | ✅ | ✅ | P-001,MKT-002 | — |
| MKT-005 | 多周期支持（1m~1d） | P0 | ✅ | ✅ | MKT-004 | — |
| MKT-006 | K 线周期重采样（timeframe_engine.py） | P0 | ✅ | ✅ | MKT-004 | — |
| MKT-007 | WS 断线自动重连（指数退避） | P0 | ✅ | ✅ | MKT-002 | — |
| MKT-008 | 重连后 K 线缺失检测与 REST 补全 | P0 | ✅ | ✅ | MKT-007,MKT-003 | — |
| MKT-009 | 多品种订阅（≥ 50 交易对） | P0 | ✅ | ✅ | MKT-002 | — |
| MKT-010 | MarketEngine 主循环（asyncio + ZMQ PUB） | P0 | ✅ | ✅ | MKT-004,T-002 | — |
| MKT-011 | K 线收盘写入 DuckDB（KlineRepository.save_batch） | P0 | ✅ | ✅ | MKT-010,R-001 | — |
| MKT-012 | 最新 K 线快照写入 Redis | P1 | ✅ | ✅ | MKT-010,DB-008 | — |
| MKT-013 | 启动时检查 K 线缺失并 REST 补全 | P0 | ✅ | ✅ | MKT-003,R-001 | — |
| MKT-014 | 行情数据校验（缺失/跳跃/0值） | P1 | ✅ | ✅ | MKT-010 | — |
| MKT-015 | Tick 数据接收（逐笔/盘口） | P1 | ✅ | ✅ | MKT-001 | — |

**MKT 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| MKT-T-001 | Binance WS 消息解析（mock 数据） | ✅ |
| MKT-T-002 | K 线标准化：各格式 → Kline 转换 | ✅ |
| MKT-T-003 | 周期重采样：1m → 5m/1h 一致性 | ✅ |
| MKT-T-004 | 断线重连状态机流转 | ✅ |
| MKT-T-005 | K 线缺失检测逻辑 | ✅ |
| MKT-T-006 | MarketEngine 集成：WS → 标准化 → ZMQ 发布 | ✅ |

---

### 2.3 指标引擎 + 信号引擎（W5-6）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| IND-001 | polars 实现 MA 指标 | P0 | ✅ | ✅ | P-007 | — |
| IND-002 | polars 实现 EMA 指标 | P0 | ✅ | ✅ | P-007 | — |
| IND-003 | polars 实现 RSI 指标 | P0 | ✅ | ✅ | P-007 | — |
| IND-004 | polars 实现 MACD 指标 | P0 | ✅ | ✅ | P-007 | — |
| IND-005 | polars 实现 BOLL 指标 | P0 | ✅ | ✅ | P-007 | — |
| IND-006 | polars 实现 ATR 指标 | P0 | ✅ | ✅ | P-007 | — |
| IND-007 | polars 实现 KDJ 指标 | P0 | ✅ | ✅ | P-007 | — |
| IND-008 | polars 实现 VWAP 指标 | P1 | ✅ | ✅ | P-007 | — |
| IND-009 | 指标注册表（registry.py） | P0 | ✅ | ✅ | IND-001~007 | — |
| IND-010 | 增量计算引擎（新 K 线仅增量更新） | P0 | ✅ | ✅ | IND-009 | — |
| IND-011 | 多周期指标独立计算 | P0 | ✅ | ✅ | IND-010 | — |
| IND-012 | 指标预热（加载历史数据初始化） | P0 | ✅ | ✅ | IND-010,R-001 | — |
| IND-013 | IndicatorEngine 主循环 | P0 | ✅ | ✅ | IND-010,T-002 | — |
| SIG-001 | 规则引擎基类（crossover/threshold/comparison） | P0 | ✅ | ✅ | P-005,IND-009 | — |
| SIG-002 | 组合条件（AND/OR/NOT） | P0 | ✅ | ✅ | SIG-001 | — |
| SIG-003 | 标准化信号生成 | P0 | ✅ | ✅ | SIG-002,P-005 | — |
| SIG-004 | 信号冷却期（去重） | P0 | ✅ | ✅ | SIG-003 | — |
| SIG-005 | 信号路由（自动/半自动/告警） | P0 | ✅ | ✅ | SIG-003 | — |
| SIG-006 | SignalEngine 主循环 | P0 | ✅ | ✅ | SIG-005,T-002 | — |

**IND 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| IND-T-001 | MA(7)/MA(25) 与手工计算一致（误差 < 0.01%） | ✅ |
| IND-T-002 | EMA 递推公式正确性 | ✅ |
| IND-T-003 | RSI(14) 边界值（全涨=100，全跌=0） | ✅ |
| IND-T-004 | MACD DIF/DEA/HIST 与参考值一致 | ✅ |
| IND-T-005 | BOLL 上中下轨计算正确性 | ✅ |
| IND-T-006 | 增量计算：全量 vs 增量结果一致性 | ✅ |
| IND-T-007 | 指标预热：MA(200) 需 ≥ 200 根 K 线 | ✅ |
| IND-T-008 | polars 性能：10000 根 K 线 MA 计算 < 100ms | ✅ |

**SIG 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| SIG-T-001 | 金叉/死叉检测正确性 | ✅ |
| SIG-T-002 | AND/OR/NOT 组合逻辑真值表 | ✅ |
| SIG-T-003 | 信号冷却期：冷却内不重复触发 | ✅ |
| SIG-T-004 | 信号路由：三种模式正确分发 | ✅ |

---

### 2.4 交易引擎 + 风控引擎（W7-8）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| TRD-001 | BinanceAdapter 下单（市价/限价） | P0 | ✅ | ✅ | MKT-001 | Executor 抽象基类 |
| TRD-002 | BinanceAdapter 撤单 | P0 | ✅ | ✅ | TRD-001 | — |
| TRD-003 | BinanceAdapter 订单/持仓/账户查询 | P0 | ✅ | ✅ | TRD-001 | — |
| TRD-004 | 订单状态机（完整流转） | P0 | ✅ | ✅ | P-003 | — |
| TRD-005 | OrderManager（订单生命周期管理） | P0 | ✅ | ✅ | TRD-004 | — |
| TRD-006 | PositionManager（持仓管理） | P0 | ✅ | ✅ | P-004 | — |
| TRD-007 | TradeEngine 主循环（信号→风控→下单） | P0 | ✅ | ✅ | TRD-005,SIG-006 | — |
| TRD-008 | 订单写入 DuckDB | P0 | ✅ | ✅ | TRD-005,R-002 | — |
| TRD-009 | 订单状态变更更新 DuckDB | P0 | ✅ | ✅ | TRD-008 | — |
| TRD-010 | 成交记录写入 DuckDB | P0 | ✅ | ✅ | R-003 | — |
| TRD-011 | 持仓快照写入 Redis | P1 | ✅ | ✅ | TRD-006,DB-008 | — |
| TRD-012 | 交易模式切换（live/paper/backtest） | P0 | ✅ | ✅ | TRD-007 | TradeMode 枚举 |
| TRD-013 | simulator.py 模拟执行器 | P0 | ✅ | ✅ | TRD-012 | 市价/限价撮合 |
| TRD-014 | 订单断线重连状态同步 | P0 | ✅ | ✅ | TRD-003,MKT-007 | — |
| RISK-001 | RISK-001: 单笔最大金额 | P0 | ✅ | ✅ | P-006 | — |
| RISK-002 | RISK-002: 单品种最大持仓 | P0 | ✅ | ✅ | P-004 | — |
| RISK-003 | RISK-003: 总持仓上限 | P0 | ✅ | ✅ | P-006 | — |
| RISK-004 | RISK-004: 单日最大亏损 | P0 | ✅ | ✅ | P-006 | — |
| RISK-005 | RISK-005: 单策略最大亏损 | P0 | ✅ | ✅ | P-006 | — |
| RISK-006 | RISK-006: 下单频率限制 | P0 | ✅ | ✅ | — | — |
| RISK-007 | RISK-007: 价格偏离保护 | P0 | ✅ | ✅ | — | — |
| RISK-008 | RISK-008~012: 最小下单量/资金检查/连续亏损/夜间限制/新品种 | P1 | ✅ | ✅ | — | — |
| RISK-009 | RiskEngine 主循环（fail-closed） | P0 | ✅ | ✅ | RISK-001~008 | — |
| RISK-010 | 风控日志写入（不可删除） | P0 | ✅ | ✅ | RISK-009,R-005 | — |
| RISK-011 | 风控规则热更新 | P1 | ✅ | ✅ | RISK-009 | — |

**TRD 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| TRD-T-001 | 订单状态机：合法流转通过 / 非法流转拒绝 | ✅ |
| TRD-T-002 | 部分成交：剩余数量正确更新 | ✅ |
| TRD-T-003 | TradeEngine：信号→风控→下单全链路（mock） | ✅ |
| TRD-T-004 | 模拟执行器撮合正确性 | ✅ |
| TRD-T-005 | 订单断线重连状态同步 | ✅ |

**RISK 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| RISK-T-001 | 12 条规则逐条验证（通过/拒绝边界值） | ✅ |
| RISK-T-002 | fail-closed：风控不可用时拒绝所有订单 | ✅ |
| RISK-T-003 | 风控检查延迟 < 1ms | ✅ |
| RISK-T-004 | 风控日志写入完整性 | ✅ |

---

### 2.5 回测引擎（W9）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| BT-001 | 撮合模拟器（matcher.py） | P0 | ✅ | ✅ | TRD-013 | 市价/限价/止损撮合 |
| BT-002 | 滑点模型（Fixed/Percentage/VolumeBased） | P0 | ✅ | ✅ | BT-001 | — |
| BT-003 | 手续费模型 | P0 | ✅ | ✅ | BT-001 | Fixed/Percentage/Tiered |
| BT-004 | 绩效分析（15 项指标） | P0 | ✅ | ✅ | BT-001 | — |
| BT-005 | BacktestEngine 主循环 | P0 | ✅ | ✅ | BT-001~004 | look-ahead bias 防护 |
| BT-006 | 回测 vs 实盘一致性验证 | P0 | ✅ | ✅ | BT-005 | — |
| BT-007 | 回测结果存储 | P1 | ✅ | ✅ | BT-005 | BacktestResult dataclass |

**BT 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| BT-T-001 | 撮合模型：市价/限价/止损单撮合规则 | ✅ |
| BT-T-002 | 滑点模型：三种模型计算正确性 | ✅ |
| BT-T-003 | 绩效指标：双均线回测与手工计算一致（误差 < 0.01%） | ✅ |
| BT-T-004 | look-ahead bias 防护：信号在收盘时产生，下根执行 | ✅ |

---

### 2.6 前端 MVP（W10）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| FE-001 | Vue 3 + Vite + TypeScript 项目初始化 | P0 | ✅ | — | — | Vite 6 + vue-tsc |
| FE-002 | TradingView Lightweight Charts 集成 | P0 | ✅ | — | FE-001 | lightweight-charts v5 |
| FE-003 | 行情看板页面（K 线 + 指标叠加） | P0 | ✅ | — | FE-002 | KlineChart 组件 |
| FE-004 | WebSocket 实时数据接入（Pinia store） | P0 | ✅ | — | FE-001 | market store + 自动重连 |
| FE-005 | 交易面板（手动下单 + 持仓 + 挂单） | P0 | ✅ | — | FE-004 | trade store + axios |
| FE-006 | 策略管理页面（基础：列表 + 启停） | P0 | ✅ | — | FE-004 | — |
| FE-007 | 账户总览页面 | P1 | ✅ | — | FE-004 | — |
| FE-008 | 深色主题 + UI 规范 | P1 | ✅ | — | FE-001 | CSS 变量体系 |

---

### 2.7 Gateway（W10）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| GW-001 | FastAPI 应用骨架 + CORS | P0 | ✅ | ✅ | B-001 | — |
| GW-002 | JWT 认证中间件 | P0 | ✅ | ✅ | GW-001 | PyJWT HS256 |
| GW-003 | 行情路由（klines/symbols/ticker） | P0 | ✅ | ✅ | GW-001,R-001 | — |
| GW-004 | 策略路由（CRUD + 启停 + 日志） | P0 | ✅ | ✅ | GW-001,R-004 | — |
| GW-005 | 交易路由（orders/positions/fills/account） | P0 | ✅ | ✅ | GW-001,TRD-007 | — |
| GW-006 | 回测路由（run/tasks/result） | P0 | ✅ | ✅ | GW-001,BT-005 | — |
| GW-007 | WebSocket 推送服务（K线/信号/订单/持仓） | P0 | ✅ | ✅ | GW-001,T-002 | 订阅/心跳 |
| GW-008 | 系统路由（health/engines/alerts） | P1 | ✅ | ✅ | GW-001 | — |

**GW 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| GW-T-001 | JWT 签发/验证/过期 | ✅ |
| GW-T-002 | 行情 API 参数校验 + 响应格式 | ✅ |
| GW-T-003 | WS 订阅/取消订阅/心跳 | ✅ |

---

### 2.8 策略框架（W9-10）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| STR-001 | Strategy SDK（TradeClient + MarketClient） | P0 | ✅ | ✅ | T-003 | — |
| STR-002 | StrategyContext（参数 + 日志 + 状态） | P0 | ✅ | ✅ | STR-001 | — |
| STR-003 | 策略进程沙箱（multiprocessing spawn） | P0 | ✅ | ✅ | STR-002 | Windows 适配 |
| STR-004 | 策略生命周期管理（加载→运行→停止） | P0 | ✅ | ✅ | STR-003 | — |
| STR-005 | 双均线示例策略（dual_ma.py） | P0 | ✅ | ✅ | STR-002 | — |
| STR-006 | 策略独立日志 | P0 | ✅ | ✅ | STR-003 | — |

**STR 模块单元测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| STR-T-001 | SDK TradeClient：下单/撤单/查询（mock） | ✅ |
| STR-T-002 | SDK MarketClient：K线/指标查询（mock） | ✅ |
| STR-T-003 | 策略沙箱：崩溃隔离（策略异常不影响主进程） | ✅ |
| STR-T-004 | 双均线策略：金叉买入 + 死叉卖出（回测验证） | ✅ |

---

### 2.9 Phase 1 集成验收

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| INT-001 | 全链路联调：行情→指标→信号→风控→下单 | P0 | ✅ | ✅ | 全部 | 3 个全链路测试 |
| INT-002 | 双均线策略 Binance 模拟盘运行 | P0 | ✅ | — | INT-001 | Paper Mode + 真实行情 |
| INT-003 | 双均线策略 Binance 实盘运行 | P0 | ⏸️ | — | INT-002 | 已列入 BACKLOG，用户决定时机 |
| INT-004 | 行情断线 10s 内自动恢复 | P0 | ✅ | ✅ | MKT-007 | 指数退避验证 |
| INT-005 | 风控规则全量验证 | P0 | ✅ | ✅ | RISK-009 | 12 规则 + fail-closed |
| INT-006 | 回测结果与手动计算一致 | P0 | ✅ | ✅ | BT-005 | 误差 < 0.01% |

**INT 模块集成测试清单：**

| 测试 ID | 测试内容 | 状态 |
|---------|---------|------|
| INT-T-001 | 全链路：K线→指标→信号 + 信号→风控→下单 + 风控拒绝 | ✅ |
| INT-T-002 | 断线重连：指数退避 10s 内多次尝试 | ✅ |
| INT-T-003 | 风控：12 规则全加载 + fail-closed 原则 | ✅ |
| INT-T-004 | 回测一致性：手动计算匹配 + 绩效分析器精度 | ✅ |

---

## 三、Phase 2：体验完善（W11-18）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| P2-001 | OKX 适配器 | P0 | ✅ | ✅ | MKT-001 | normalizer+OKXAdapter(WS/REST)+OKXExecutor，30测试 |
| P2-002 | 合约支持（杠杆/做空/资金费率） | P0 | ✅ | ✅ | P2-001 | BinanceFuturesAdapter+FuturesExecutor+FundingMonitor，25测试 |
| P2-003 | 前端信号监控页面 | P1 | ✅ | — | FE-004 | SignalView.vue（实时信号流+WS推送+确认下单） |
| P2-004 | 前端回测报告可视化 | P1 | ✅ | — | FE-004,BT-005 | BacktestView.vue（8项绩效+SVG资金/回撤曲线+交易明细） |
| P2-005 | 前端风控面板 | P1 | ✅ | — | RISK-009 | RiskView.vue（12规则管理+触发记录+概览统计） |
| P2-006 | 前端系统监控页面 | P1 | ✅ | — | GW-008 | SystemView.vue（引擎状态+资源占用+WS统计+告警） |
| P2-007 | 策略热加载 | P1 | ✅ | ✅ | STR-004 | StrategyHotLoader（目录监控+动态加载+热替换+版本回滚），9测试 |
| P2-008 | 参数优化回测（网格/随机搜索） | P1 | ✅ | ✅ | BT-005 | ParameterOptimizer（网格/随机搜索+并行执行+walk-forward），10测试 |
| P2-009 | 告警通知（Webhook：钉钉/飞书/Telegram） | P1 | ✅ | ✅ | — | AlertManager+4渠道(DingTalk/Feishu/Telegram/Webhook)+规则+升级，15测试 |
| P2-010 | 性能压测与优化 | P1 | ✅ | ✅ | INT-001 | 16项压测全通过（指标计算/回测引擎/数据标准化/内存/并发） |

### 3.1 前后端数据贯通（Phase 2 补充）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| P2-011 | Gateway 共享状态模块（AppState） | P0 | ✅ | — | GW-001 | state.py：HTTP客户端复用+AlertManager单例+币安配置 |
| P2-012 | system 路由接入真实数据 | P0 | ✅ | — | P2-011 | psutil 真实CPU/内存/磁盘+引擎状态+告警列表 |
| P2-013 | trade 路由接入币安 Demo 账户 | P0 | ✅ | — | P2-011 | HMAC签名+余额/持仓/下单/撤单/成交记录 |
| P2-014 | alert 路由（告警中心 API） | P0 | ✅ | — | P2-009 | 事件/规则/渠道 CRUD+测试告警 |
| P2-015 | WebSocket K线实时推送 | P0 | ✅ | — | GW-007 | ws_kline.py 后台任务（5s轮询币安→广播订阅者） |
| P2-016 | 前端告警中心页面（FE-008） | P1 | ✅ | — | P2-014 | AlertView.vue（事件列表/规则管理/渠道配置） |
| P2-017 | AccountView 接入真实账户数据 | P1 | ✅ | — | P2-013 | 资产明细表+持仓列表+USDT余额 |
| P2-018 | SystemView 接入真实系统指标 | P1 | ✅ | — | P2-012 | 5秒自动刷新，移除硬编码数据 |
| P2-019 | MarketStore WS实时推送+REST降级 | P0 | ✅ | — | P2-015 | WS订阅+断线重连+REST降级轮询+Ticker 15s |
| P2-020 | 路由+导航注册告警中心 | P1 | ✅ | — | P2-016 | /alerts 路由+侧边栏入口 |

---

## 四、Phase 3：扩展与稳定（W19-26）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| P3-001 | A 股适配（QMT/miniQMT） | P1 | ⏸️ | 🧪 | MKT-001 | 需券商资质，已列入 BACKLOG |
| P3-002 | 期货适配（CTP） | P1 | ⏸️ | 🧪 | MKT-001 | 需期货账户，已列入 BACKLOG |
| P3-003 | 高级风控（多层级/动态规则） | P1 | ✅ | ✅ | RISK-009 | RiskEngine 增强：多层级规则链+动态参数热更新+账户快照 |
| P3-004 | 安全加固（TLS 1.3 / AES-256） | P1 | ✅ | ✅ | GW-002 | AES-256-GCM 加密存储 API Key+JWT+TLS 配置+密钥脱敏 |
| P3-005 | 运维监控（健康检查/自动备份） | P1 | ✅ | ✅ | — | health_check.py（5项检查）+backup_duckdb.py（Parquet备份/恢复/保留策略），14测试 |
| P3-006 | 混沌测试 | P2 | ✅ | ✅ | — | ChaosMonkey 框架（7种故障类型+概率触发）+网络/API/数据混沌测试，30测试 |

### 4.1 质量保障（Phase 3 补充）

| ID | 任务 | 优先级 | 状态 | 测试 | 依赖 | 备注 |
|----|------|--------|------|------|------|------|
| QA-001 | E2E 测试框架（Playwright） | P1 | ✅ | ✅ | FE-009 | 导航/策略/回测/设置/API/响应式，18项测试 |
| QA-002 | 全量单元测试回归+覆盖率 | P0 | ✅ | ✅ | 全部 | 490项通过，83%覆盖率 |
| QA-003 | K线图与指标副图同步联动 | P1 | ✅ | ✅ | FE-003 | useChartSync 时间轴+十字光标双向同步 |

---

## 五、里程碑

| 里程碑 | 目标 | 验收标准 | 预计时间 | 状态 |
|--------|------|---------|---------|------|
| M0 | 环境就绪 | uv/ClickHouse/Redis 全部可用 | W1 | ✅ |
| M1 | 骨架搭建完成 | 目录结构 + 数据结构 + 通信层可运行 | W2 | ✅ |
| M2 | 行情可用 | Binance WS 接入 + K 线持久化 | W4 | ✅ |
| M3 | 指标+信号可用 | 控制台可见交易信号 | W6 | ✅ |
| M4 | 交易闭环 | 模拟盘下单成功 + 风控生效 | W8 | ✅ |
| M5 | 回测可用 | 双均线回测结果与手工一致 | W9 | ✅ |
| M6 | Phase 1 验收 | 双均线 Binance 实盘运行 | W10-14 | 🔄（待 INT-003 实盘） |
| M7 | Phase 2 验收 | OKX + 合约 + 前端完善 | W18-20 | ✅ |
| M8 | Phase 3 验收 | A 股/期货 + 安全加固 | W24-28 | 🔄（安全/风控/运维✅，A股期货待启动） |
| M9 | v1.0.0-paper 发布 | 模拟盘+回测可用版打包 | 2026-07-29 | ✅ |

---

## 六、统计面板

| 类别 | 总计 | ⬜ 未开始 | 🔄 进行中 | ⏸️ BACKLOG | ✅ 完成 | ❌ 失败 |
|------|------|---------|---------|-----------|--------|--------|
| Phase 1 开发任务 | 108 | 0 | 0 | 1 | 107 | 0 |
| Phase 1 单元/集成测试 | 62 | 0 | 0 | 0 | 62 | 0 |
| Phase 2 任务 | 20 | 0 | 0 | 0 | 20 | 0 |
| Phase 2 单元/压测 | 50 | 0 | 0 | 0 | 50 | 0 |
| Phase 3 任务 | 6 | 0 | 0 | 2 | 4 | 0 |
| 质量保障（E2E/回归/图表同步） | 3 | 0 | 0 | 0 | 3 | 0 |
| **总计** | **249** | **0** | **0** | **3** | **246** | **0** |

> BACKLOG 项：INT-003 实盘验证、P3-001 A股适配、P3-002 期货适配（用户明确推迟，不阻塞主线）

---

## 七、变更记录

| 日期 | 版本 | 变更内容 |
|------|------|---------|
| 2026-07-29 | v2.0 | 创建，含全三阶段任务分解、单元测试清单、交付原则 |
| 2026-07-29 | v2.1 | Phase 1 基础设施+B-001~B-007✅ + P-001~P-010✅（45测试全通过） |
| 2026-07-29 | v2.2 | 架构变更：ClickHouse 无 Windows 原生客户端，Phase 1 统一用 DuckDB 替代；数据库任务从 12 项精简为 8 项（DuckDB+Redis） |
| 2026-07-29 | v2.3 | T-001~T-005✅（27测试全通过）：Transport 抽象基类 + ZMQ PUB/SUB + DEALER-REP + PortRegistry；conftest 修复 SelectorEventLoop |
| 2026-07-29 | v2.4 | DB-001~DB-008✅（31测试全通过）：DuckDBManager 单例+写锁+10表Schema+BatchWriter+RedisCacheManager；全量103测试通过 |
| 2026-07-29 | v2.5 | R-001~R-006✅（27测试全通过）：6 个 Repository（Kline/Order/Fill/Strategy/RiskLog/Tick）；Schema 修复 created_at→BIGINT；全量 130 测试通过 |
| 2026-07-29 | v2.6 | C-001~C-005✅（28测试全通过）：loguru 日志配置+settings.yaml+exchanges.yaml+pydantic-settings 加载器+GracefulShutdown；全量 158 测试通过 |
| 2026-07-29 | v3.0 | MKT-001~MKT-015✅（25测试全通过）：ExchangeAdapter 基类+BinanceAdapter(WS/REST/重连/多品种)+K线标准化+周期重采样+MarketEngine 主循环；HTTP代理 127.0.0.1:7897 可用；全量 183 测试通过 |
| 2026-07-29 | v3.1 | IND-001~013✅ + SIG-001~006✅（27+30测试全通过）：8 个 polars 指标(MA/EMA/RSI/MACD/BOLL/ATR/KDJ/VWAP)+指标注册表+增量计算引擎+指标预热+规则引擎(Crossover/Threshold/Comparison)+组合条件(AND/OR/NOT)+信号冷却期+信号路由(AUTO/SEMI/ALERT)+SignalEngine 主循环；全量 240 测试通过 |
| 2026-07-29 | v3.2 | TRD-001~014✅ + RISK-001~011✅（25+20测试全通过）：Executor 抽象基类+Simulator 模拟执行器(市价/限价撮合)+OrderManager 订单生命周期+PositionManager 持仓管理+TradeEngine 主循环(信号→风控→下单)+TradeMode(live/paper/backtest)+12 条风控规则+RiskEngine(fail-closed)+风控日志+热更新；全量 285 测试通过 |
| 2026-07-29 | v3.3 | BT-001~007✅ + STR-001~006✅ + GW-001~008✅（20+17+21测试全通过）：回测引擎(Matcher撮合+3种滑点模型+3种手续费模型+15项绩效指标+BacktestEngine主循环+look-ahead bias防护)+策略框架(TradeClient/MarketClient SDK+StrategyContext+进程沙箱 spawn+StrategyManager生命周期+DualMA示例策略)+API网关(FastAPI+CORS+JWT认证+5组路由+WebSocket推送)；全量 343 测试通过 |
| 2026-07-29 | v4.0 | FE-001~008✅ + INT-001,004~006✅（8集成测试全通过）：前端 MVP(Vue3+Vite6+TS+Pinia+VueRouter+LightweightCharts v5+深色主题+4页面)+集成验收(全链路联调+断线重连+风控全量验证+回测一致性)；修复 AvailableBalanceRule 市价单估算；全量 351 测试通过；Phase 1 代码开发完成(106/108)，仅剩 INT-002/003 实盘验证 |
| 2026-07-29 | v4.1 | INT-002✅：双均线策略 Binance 模拟盘运行成功（Paper Mode）；BinanceAdapter WS 代理支持(websockets proxy参数)+is_closed 字段修复(x字段)；实时行情→MA7/MA25→信号→风控→模拟下单全链路验证；全量 351 测试通过 |
| 2026-07-30 | v5.0 | Phase 2 全部完成✅：P2-001 OKX适配器(normalizer+WS/REST+Executor，30测试)+P2-002 合约支持(FuturesAdapter+Executor+FundingMonitor，25测试)+P2-003~006 前端4页面(Signal/Backtest/Risk/System)+P2-007 策略热加载(目录监控+动态加载+版本回滚)+P2-008 参数优化器(网格/随机搜索+walk-forward)+P2-009 告警通知(AlertManager+4渠道Webhook)+P2-010 性能压测(16项全通过)；全量 456 测试通过 |
| 2026-07-30 | v6.0 | 前后端数据贯通✅：P2-011~020（Gateway AppState+system/trade/alert路由接入真实数据+WS K线推送+AlertView告警中心页面(FE-008)+AccountView真实账户+SystemView真实指标+MarketStore WS+REST降级）；FE-001~FE-009全部9个前端页面完成；币安Demo账户实时数据展示验证通过 |
| 2026-07-29 | v7.0 | Phase 3 核心✅：P3-003 高级风控+P3-004 安全加固(AES-256-GCM)+P3-005 运维监控(health_check/backup_duckdb，14测试)+P3-006 混沌测试(ChaosMonkey，30测试)；QA-001 E2E测试(Playwright 18项)+QA-002 全量回归(490项通过，83%覆盖率)；INT-003/P3-001/P3-002 列入 BACKLOG |
| 2026-07-29 | v7.1 | QA-003 K线图与指标副图同步联动✅：useChartSync 组合式模块（时间轴缩放/滚动双向同步+十字光标联动+WeakSet防重复订阅）；vue-tsc+vite build 通过；全量 490 测试通过 |
| 2026-07-29 | v8.0 | 🎉 v1.0.0-paper 正式发布：版本号升级（后端/前端 1.0.0）+全量回归490项通过+前端生产构建+部署脚本(start_all/stop_all.ps1)+打包脚本(build_release.ps1)+CHANGELOG.md；产物 release/klinequant-1.0.0-paper.zip（231条目，0.49MB，已排除.env/密钥/运行时数据）；实盘验证(INT-003)延至 v1.1 |
