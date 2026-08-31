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
| 2026-07-29 | v8.0 | 🎉 v1.0.0-paper 正式发布：版本号升级（后端/前端 1.0.0）+全量回归490项通过+前端生产构建+部署脚本(start_all/stop_all.ps1)+打包脚本(build_release.ps1)+CHANGELOG.md；产物 release/klinequant-1.0.0-paper.zip（231条目，0.49MB，已排除.env/密钥/运行时数据）；实盘验证(INT-003)延至 v1.1；已发布至 GitHub（https://github.com/mryuyu/klinequant，tag v1.0.0-paper）；新建《KlineQuant 迭代计划.md》规划 v1.1~v2.0 路线 |
| 2026-08-09 | v8.1 | 迭代计划更新至 v1.9：UI 重新设计（UI-001~005）标记完成（lc-live.html 唯一迭代基线，随 v1.1.0-mockup~v1.3.0-mockup 系列发布，最新 tag v1.3.0-mockup）；MKT-PLG 市场源插件框架+IG 外汇接入已验证闭环（单测 504 passed，commit f34374c 已推送）；新增 MKT-IB 盈透证券接入任务（下一轮，按插件框架，先 Paper） |
| 2026-08-09 | v8.2 | 迭代计划更新至 v2.0：指标契约定稿——计算契约 key=(指标名,参数组合) 天然支持多参数/多实例；display_meta 扩展 range 值域字段；indLayout 升级为 [{name,params,group?}] 支持同窗格叠加多指标（如 KD+RSI）；IND-109 补充策略自定义指标注册规则（命名前缀/JSON 可序列化/状态可快照/先注册后预热/卸载后 Registry 保留） |
| 2026-08-09 | v8.3 | 迭代计划更新至 v2.1（MACD 倍数族样本驱动定稿）：IND-102 历史深度按订阅集合最大参数需求拉取（warmup_max）+剔除预热段+预热不足降级；IND-103 family 指标族机制（base+mults 展开/联动重算）+fields 字段绘制选择器（HIST 仅 1x）+共享零轴参考线+线型体系（弹窗下拉含阶梯线用户自选，display_meta 字段级默认 MA 实线/EMA 虚线）；IND-106 df 列命名 slug 规则；M2.5 里程碑增 MACD 倍数族验收样例；MKT-IB 增补历史深度验收硬性项 |
| 2026-08-09 | v8.4 | IND 首个里程碑落地（MACD 样本三端闭环已验证，迭代计划更新至 v2.2）：① IND-101 MACD O(1) 真增量（对齐 polars ewm_mean(adjust=False)，快照法=当前 bar 之前状态/新 ts 隐式提交/同 ts 幂等）；② IND-102 /api/indicator/meta + /api/indicator/history（契约 key 幂等预热、拉取深度 min(1000, need+min_periods)、剔除预热段）+ WS indicators.{exchange}.{symbol}.{tf} 增量推送；③ IND-106 样本级：require_indicators 声明 + MarketClient.get_indicator_history；④ IND-103 MACD 副图改后端 REST+WS（本地兜底+loadGen 防竞态）；⑤ 验证：新增引擎 8 用例 + 网关 7 用例，全量回归 519 passed；浏览器实测 MACD 副图后端渲染（inst.backend=true 非兜底）、170s 捕获 46 条 indicators.* WS 消息图例实时跳动、控制台零报错 |
| 2026-08-10 | v8.5 | 修复 MACD 副图懒加载翻页后历史段空白问题：根因是 ensure_warmed “已预热即返回”导致序列深度锁死在首屏预热深度（前端翻页后 limit 变大但后端不加深）。indicator_service 改为按显示需求深度判断（series_len >= need 才复用）+ _fetch_depth 从最新向过去分页拉取（单页 1000，总深度上限 5000）；前端 loadMore→renderChart 已自动带新 klines.length 重新请求无需改动。新增网关用例 test_history_deepens_on_larger_limit（_FakeSource 支持 end_time），全量回归 520 passed；真实币安实测 300→1300 加深成功，浏览器实测翻页至 1500 根后最左侧 K 线 MACD 图例有值 |
| 2026-08-10 | v8.6 | 修复价格坐标/图例精度与真实价格不一致（BTC 被量级启发式截成 1 位小数）：lc-live.html 新增 detectPrecision（取样最近 120 根 OHLC 最大小数位，上限 8、整数价保底 1）替换原量级启发式，坐标轴 priceFormat 与图例 fmt（curPrec）同源；顶栏 24h 高低随 fmt 自动一致。浏览器实测：BTC 全位 2 位小数、DOGE 切品种后变 5 位、控制台零报错；附带观察：品种列表/持仓表价格沿用当前图精度（如 BTC 显 5 位），如需按品种各自格式化另行优化 |
| 2026-08-10 | v8.7 | 架构纠正（用户铁律：前端价格必须按订阅到的价格显示，前端只做渲染）：精度推导从前端移至后端市场源——MarketSource 基类新增 price_decimals（原始价格去尾零计小数位）+ _track_prec 品种精度缓存（只增不减）+ price_precision 查询；币安源在 REST klines/ticker、WS 实时 bar、REST 降级四路从原始字符串累积精度，IG 源从归一化后价格累积（点位对已÷10000 回汇率）；/api/market/klines 与 /api/market/ticker 响应新增 price_precision 字段；前端 fetchPage 携带精度，优先消费后端值，detectPrecision 仅在后端未下发时兜底。新增 4 用例（含下发契约），全量回归 524 passed；实测 BTC=2/DOGE=5 下发正确，前端 curPrec 与后端一致 |
| 2026-08-10 | v8.8 | 精度原则扩展至所有价格展示位（用户铁律：品种列表/持仓列表等任何涉及价格精度的地方一律按订阅价格为标准）：前端新增按品种 symbolPrec 表（fetchPage/fetchTopTicker/品种列表 ticker 三处只记录后端下发的 price_precision，不推导）+ fmtSym(sym, v) 统一格式化入口（品种列表/持仓表最新价均改走按品种精度渲染，未知品种兜底当前图精度；无订阅的静态示例组按数据自身小数位渲染）；修复 v8.6 附带观察：切 DOGE 后 BTC 行不再显 5 位尾零。浏览器实测：品种列表 BTC 2 位/DOGE 5 位/XRP 4 位各自独立，持仓表 BTC 65,203.50（2 位）/USDC 1.00065（5 位），symbolPrec={BTCUSDT:2,…,DOGEUSDT:5}，控制台零报错 |
| 2026-08-10 | v8.9 | 修复 IG 外汇价格精度被推高到 8 位问题（用户报告）：根因是点位归一化用浮点除法产生噪声（11514.8/10000=1.1514799999999998），price_decimals 按 repr(float) 计位把 EURUSD 精度推到 8 位且缓存只增不减。修复：ig_client.normalize_rate 改用 Decimal 十进制移位（scaleb(-4)）替代浮点除法，归一化结果 repr 干净；ig_source ticker bid/ask 中点改 Decimal 均值防长尾；_ticker_from_candles 路径补 _track_prec（snapshot 无报价时此路径是前端唯一下发源）。新增用例 test_normalize_rate_no_float_noise（repr 断言 + 归一化后精度 ≤5），全量回归 525 passed；实测 EURUSD prec=5（low_24h 1.15148 干净）、USDJPY prec=3、GBPUSD prec=5，浏览器实测 EURUSD Y 轴/图例/最新价全 5 位、BTC 仍 2 位不受影响 |
| 2026-08-10 | v9.0 | 新增本地 MetaTrader 5 市场源插件（用户指令：IG 历史数据量太少，增加本地 MT5 数据源）：新增 gateway/market_sources/mt5_source.py——官方 MetaTrader5 包连本机终端（IC Markets Global，实测 connected），Mt5Api 驱动层可注入 fake；历史 K 线 copy_rates_from_pos（最新 limit 根）+ copy_rates_range（end_time 翻页加深，回溯窗口 2 倍冗余）；终端轮询实时流（2s，包无推送接口，manager 去重签名识别 OHLC 变化）；精度铁律落地：symbol_info().digits 作为 price_precision 直接下发（订阅到的市场元数据，不推导，实测 EURUSD=5/USDJPY=3/USDCHF=6/XAUUSD=2）；成交量 real_volume 优先、为 0 时用 tick_volume（supports_volume=True，与 IG 无成交量形成对比）；无 tick 报价时 K 线构造 ticker 兜底；断连重连带 30s 冷却。关键限制：MetaTrader5 包单连接非线程安全，驱动层全局 threading.Lock 串行（并发直调会挂起）+ ticker 5s 缓存降压。注册：bootstrap 按 KQ_MARKET_SOURCES（默认 binance,ig,mt5），终端未连接时跳过；pyproject 新增 mt5 可选依赖；.env 新增 MT5_TERMINAL_PATH/LOGIN/PASSWORD/SERVER（全可选）。实测：M1 1000 根、D1 1000 根回溯至 2022-09；浏览器全过：品种列表 6 品种有价、翻页 1500 根无缺口、D1 最早 2020-10（前端 1500 上限约束）、VOL 有数据、切回 Binance 不受影响；并发压测 6 ticker+klines 15.3s 全成功；新增 11 用例，全量回归 536 passed。附带观察：本机终端报价停在 2026-08-07（周五）收盘（数据流未更新，非插件问题）；前端轮询积压曾致 K 线请求排队（锁串行后缓解，如仍明显可降低轮询频率） |
| 2026-08-10 | v9.1 | 前端时间显示改中式（用户要求：周几 + 年/月/日 + 24 小时制，日线及以上只显示日期）：lc-live.html 新增 fmtTimeCn（中文周几表 WD + TF_DATE_ONLY={1d,3d,1w}）；chart 选项新增 localization.timeFormatter（十字线标签完整中式格式，日线及以上只到日期）+ timeScale.tickMarkFormatter（X 轴刻度紧凑格式：2026年/2026/08/08/10/14:30，消除英文月份）。浏览器实测：15m 十字线"周日 2026/08/09 12:45"、1d 十字线"周一 2025/11/24"（无时间）、X 轴无 Aug 类英文月份、控制台零报错。本次全部改动已提交并推送（commit 7a306bf，feat: v9.1 本地MT5市场源接入 + 价格精度全链路闭环 + 中式时间显示，36 文件 +2064/-84；.gitignore 新增 _dbg_*.py 与 .git_commit_msg.tmp 防误提交） |
| 2026-08-10 | v9.2 | MT5 终端轮询间隔 2s → 0.5s（用户要求：tick 更新改成 500ms，与币安 K 线更新节奏对齐）：mt5_source.POLL_INTERVAL 默认值改 0.5（MT5_POLL_INTERVAL 可覆盖）；.env.example 补配置说明。每轮每目标仅 copy_rates_from_pos(count=1) 轻量本地调用，锁串行无压力；manager OHLC 签名去重不变（仅变化才广播）。MT5 单测 11 passed；浏览器实测：高分辨率采样出现 400-900ms 级亚秒跳动（旧 2s 轮询下最小间隔必 ≥2000ms）、WS 延迟 0-1ms 无断连、品种列表正常刷新、控制台零报错。未提交 git |
| 2026-08-10 | v9.3 | 品种列表价格改走 WS 推送（用户选定方案 1：与图表同源同频，替代 REST 轮询）：lc-live.html 新增 listPriceTopics 集合 + resyncListTopics()（按当前所列表品种差量订阅/退订 klines.{ex}.{sym}.1m 主题，后端 ws 已支持 unsubscribe）+ updateListPrice()（定点改 DOM 行价格不重建列表，涨/跌色闪）；接入点：首屏 initSources/切所 switchExchange/增删品种/WS 重连 onopen（clear+全量重建，服务端订阅已丢）；onmessage 命中列表主题时更新且不 return（当前品种 1m 时图表链路继续处理）；refreshListPrices REST 轮询 10s→30s 仅兜底涨跌幅。浏览器实测：列表 6 品种亚秒~秒级跳动（12s 内 3-11 次，旧行为 10s 一跳）、订阅帧 6 个 klines.mt5.*.1m 正确、ticker REST 精确 30s 一批、切所往返退订/重订无残留（Binance 列表同样实时）、WS 延迟 2ms、控制台零报错。未提交 git |
| 2026-08-10 | v9.4 | 新增红涨绿跌配色方案（用户要求：全局有效）：lc-live.html 新增 COLOR_SCHEMES 配色表（gd 绿涨红跌默认/cn 红涨绿跌）+ applyColorScheme()（交换 CSS 变量 --up/--down 与新增 --up-rgb/--down-rgb 覆盖全站 UI + K 线系列 applyOptions 改色 + renderChart 全量重绘）；成交量柱/MACD 柱硬编码色改走 volColor()/histColor() 随方案；买卖按钮/删除按钮等 rgba 背景改 var(--up-rgb/--down-rgb)；开关入口：右键图表设置新增"涨跌配色"下拉框，localStorage kq-color-scheme 持久化刷新保持。连接状态色（WS 绿点/信号图标）固定不随方案交换（语义为链路状态非涨跌；修复 setWs 内联样式覆盖问题）。浏览器实测：K 线/量柱/MACD/顶栏/品种列表方向色逐像素验证全对、双向切换+刷新持久化均生效、控制台零报错。未提交 git |
| 2026-08-11 | v9.5 | 指标参数齿轮移至指标名称前面（用户要求：前端多处微调之一）：lc-live.html 图例 rowHtml 渲染顺序改为 齿轮按钮 → 指标名称 → 数值标签（原 名称 → 数值 → 齿轮）；点击委托按 data-ind-id + closest('.gear-btn') 定位，与位置无关。验证：主图 MA/BOLL、副图 MACD/KD/RSI 共 5 个指标齿轮均在行首，点击弹窗正常。未提交 git |
| 2026-08-11 | v9.6 | 前端市场化改造（用户原则：市场固定为加密/外汇/股票/期货，从策略到前端不感知数据源，一市场绑定唯一数据源，国内/国外两大分类；布局照搬 TradingView 品种搜索弹窗模式）：lc-live.html 新增品种搜索弹窗（顶栏品种按钮与右栏 ＋ 为唯一入口）——市场分类水平页签（全部/加密/外汇/股票/期货，股票期货带"接入中"角标）+ 国内/国外分段切换（仅具体市场页签下出现，仿 TradingView All sources 规则）+ 搜索框过滤 + 结果行（图标/符号/名称/市场标签）；选中后按市场路由切换数据源与品种（switchExchange 新增 opts.symbol 支持），新品种自动入列表首组并 WS 订阅。市场路由阶段一为前端映射 MARKET_ROUTE={crypto:binance, fx:mt5}（IG 为外汇备用源，阶段二迁后端路由表）。移除右栏交易所切换器（renderExchSwitch/sc-exch 整套）；顶栏 SPOT 标签改显示市场名（updateTopTag），LIVE 徽标不再显示数据源名称；WS 连接状态从顶栏移至右下角状态栏；原手动添加品种弹窗保留，入口移至搜索弹窗底部链接。未接入市场（股票/期货、国内全部）显示"数据源接入中"占位。全量回归 536 passed。待用户验证清单确认。未提交 git |
| 2026-08-11 | v9.7 | 市场化改造验收反馈四项修正：① 品种搜索弹窗去掉提示文字（底部"市场固定…数据源对用户隐藏"、页签下"一市场一数据源（后端路由，前端无感）"及全部页签提示，sr-foot/sr-hint 整套移除）；② 顶栏 LIVE 徽标整个移除；③ 手动添加品种功能移除（sym-modal 弹窗与相关 JS 整套删除，品种全部由后端提供，右栏 ＋ 仅打开品种搜索弹窗）；④ 状态栏右下角数据源名称改为实际名称：后端 mt5_source.label "MT5 Local"→"IC Markets"（状态栏 sbInfo 与品种列表默认分组名同源生效）。全量回归 536 passed，后端已重启（PID 2840）。追加修正：右栏品种列表默认分组名改用市场分类（groupsFor 取 MARKET_NAMES[MARKET_OF_EX[ex]]，外汇分组显示"外汇"而非数据源名 IC Markets；市场路由常量块前移至品种列表段之前，消除 localStorage 恢复外汇时的初始化时序隐患）。未提交 git |
| 2026-08-11 | v9.8 | 后端全量品种目录 + 搜索弹窗 TradingView 式资产分类（用户要求：后端读取全部品种并按属性/数据源分类，如 EURUSD(外汇,IC Markets)、XAUUSD(贵金属,IC Markets)，区分不同数据源的相同品种，搜索框内增加分类页签）：后端 MarketSource 基类新增可选 list_symbols()（默认返回 default_symbols 带 type）；Mt5Source 实现终端 symbols_get 全量枚举（trade_mode=FULL 过滤 6523 个，path 顶层目录归资产类别 forex/metal/index/commodity/crypto/stock/bond，Commodities 二级细分 Metals=贵金属）；BinanceSource 实现 exchangeInfo（TRADING+USDT 计价 489 个）；IG 维持静态目录；manager 新增 list_symbols（30 分钟 TTL 缓存）；/api/market/symbols 升级为全量目录（每行 exchange/source/type，拉取失败回退默认品种；实测 7018 个，EURUSD/XAUUSD 在 IG 与 IC Markets 各一行）。数据源展示名统一：Binance Spot→Binance、IG Forex→IG。前端品种搜索弹窗重写：页签改为资产类别 pill（全部+数据中实际存在的类别，固定顺序）+ 国内/国外分段保留；首次打开拉取 /api/market/symbols 并缓存；结果行带类型标签+行尾数据源名（仿 TradingView）；同名品种多数据源为多行，选中即路由到该行数据源；单次渲染上限 200 行+"共 N 个"提示；移除 MARKET_TABS/MARKET_ROUTE 旧常量。新增单测：MT5 path 分类/回退、base 默认实现、manager TTL 缓存、币安 exchangeInfo 解析。全量回归 542 passed，后端已重启（PID 22696）。未提交 git |
| 2026-08-11 | 方案评估 | 期货分类维持现状（IC Markets 终端内期货 CFD 按标的归入指数/商品，不单独成类；真期货源接入后"期货" pill 随数据自动出现）。CTP 接入方案评估（仅讨论未实施）：选型 openctp-ctp（PyPI Windows wheel，CTPAPI 协议封装）+ openctp TTS 7×24 仿真环境开发；核心难点为 CTP 无历史 K 线接口（拟自建累积：tick→1m 聚合→DuckDB 落库，可选三方 bootstrap 打底）+ 回调线程桥接 asyncio + 日盘/夜盘时段对齐 + 月份合约主力连续映射；前置小改：源元数据增加 region 字段（cn/global）驱动前端国内/国外分段；实施分三期（A 只行情/B 主力连续与夜盘/C TraderApi 下单），待用户确认后启动 |
| 2026-08-11 | 推送记录 | v9.5~v9.8 批量提交并推送（单 commit，改动交织于同批文件）：前端市场化改造（TradingView 式品种搜索弹窗）+ 后端全量品种目录（MT5 6523 + 币安 489 + IG 6，资产类别分类 + 多源同名品种区分）+ 单测 542 passed。标签 v1.4.0-mockup。market-ui-demo.html（布局讨论产物）不入库 |
| 2026-08-11 | v9.9 | 指标引擎实质性测试：MACD 倍数族策略端到端（用户指令：MACD 4 组 1x~64x，fast=2/slow=5/m=3，验证引擎多实例处理能力；即迭代计划 M2.5 端到端验收样例）。新增：① strategies/macd_family.py 倍数族共识策略（on_init 声明 4 组 MACD 实例，≥3 组 HIST 同向出 LONG/SHORT）；② core/strategy_engine/wiring.py（IND-106 消费链路打通：consume_requirements 幂等注册 + warmup_from_df 分组预热 + inject_indicators 按 timestamp 左连接注入 df 列；列名 slug 如 macd_f128_g192_s320_hist，参数按键名排序；导入即注册全部内置指标）。引擎多实例修复：warmup 结果双键写入（指标名兼容 + ind_key 精确）、get_indicator_value 新增 params 精确定位、get_all_values 补 ind_key 键、新增 indicators_for 实例枚举。测试：tests/unit/test_macd_family.py 8 用例（800 根随机游走数据，4 实例序列与 polars 全量参考逐点对拍、增量推送后尾部仍一致、预热不足降级列为 null、on_bar 全循环信号与参考逐 bar 对拍且 LONG/SHORT 均出现）。全量回归 550 passed。未提交 git |
| 2026-08-22 | v9.10 | IND-110 def 式指标语言落地（用户拍板：策略研究自定义指标零增量改造，Python def 一个函数即可，前端展示）：① 后端新增原语计算图 core/indicator_engine/graph/——nodes.py（约 620 行）双语义原语（batch 返回 polars Expr 预热批量 + incr O(1) 标量递推实时 + snapshot/restore 复用 IND-101 快照协议，已提供 ema/sma/rolling_max/min/std/shift/cum_sum/maximum/minimum/where/abs_ + 运算符重载），Graph 调度（创建序=拓扑序，min_periods 结构推导，图级预热门控不输出失真数据），dsl.py GraphIndicator 适配层（supports_incremental=True 走引擎增量路径绕开内置列名硬编码）+ @pyindicator 装饰器注册（参数默认值来自函数签名）；② klinequant/custom_indicators/ 目录 pkgutil 自动加载，样例 TRIX（副图）/DEMA（主图叠加），网关 state.py 初始化时导入注册；③ 前端 lc-live.html：/api/indicator/meta 驱动 BACKEND_META，选择面板新增“自定义/后端”分组自动发现，每字段一条线通用渲染（pane 由 display_meta 决定）+ REST 历史 + WS 增量逐字段更新 + 参数弹窗由 default_params 动态生成 + 图例末值缓存 + indLayout 持久化兼容 {type:'custom', name}；④ 直代码约束：分支/循环可依赖参数不可依赖行情数据，数据依赖选择用 where；null 语义对齐 polars（ewm_mean ignore_nulls）；⑤ 新增单测 tests/unit/test_graph_indicator.py 8 用例（原语批量 vs 增量逐点一致、shift+EMA 前导 null 对齐、TRIX/DEMA 注册与 meta、快照同 ts 幂等、引擎集成），全量回归 558 passed。未提交 git |
| 2026-08-22 | v9.11 | IND-110 配套前端修复与体验优化（均落地 lc-live.html）：① 参数弹窗新增 ✕ 关闭按钮（复用品种搜索弹窗 sr-head/sr-close 样式）；② 遮罩误关修复 maskDismiss：输入框内选取文本拖出弹窗外松开时 click 目标落在遮罩导致误关，改为仅当按下点也在遮罩上才关（指标参数弹窗与选择面板同修）；③ 切周期/品种体验：不再 fitContent 压缩（首屏才铺满，此后用 getVisibleLogicalRange 记录显示跨度并恢复，可见根数不变即缩放比例不变，最新 K 线右对齐留白）+ 后端指标 reset 时立即清空旧周期数据、Promise.all 全部就绪后同帧批量显示（消除逐条跳入的“动画计算”观感）；懒加载翻页走非 reset 路径不受影响；④ 踩坑：lightweight-charts v5 移除了 v4 的 timeScale().barSpacing() getter 与 barSpacing 选项，调用抛 TypeError 被 load() 兜底 catch 吞掉误显示“后端未连接”，已改逻辑视窗方案并在 catch 补 console.error 暴露真实错误；⑤ 排障：8090 端口曾被凌晨残留旧静态服务进程（系统 Python）占用导致新网关绑定失败退出、旧页面硬编码 8000 提示后端未连接，杀旧进程重启恢复；⑥ BUG-001 记录于迭代计划 2.4 节：改单指标参数全量重建（rebuildIndicators），待体感卡顿明显时改单实例重建。全量回归 558 passed |
| 2026-08-22 | 推送记录 | v9.9~v9.11 批量提交并推送（commit 4d9c88e，16 文件 +1991/-32）：IND-110 def 式指标语言（原语计算图 + custom_indicators 自动加载 + 前端 meta 动态目录）+ MACD 倍数族策略与 wiring 接线 + 前端体验修复（✕ 按钮/遮罩误关/切周期保持比例与指标一次显示/v5 barSpacing 踩坑）。标签 v1.5.0-mockup。market-ui-demo.html（布局讨论产物）不入库 |
| 2026-08-25 | v9.12 | 删除 IG 外汇市场源插件（用户指令）：删除 gateway/market_sources/ig_source.py 与 ig_client.py（含 Lightstreamer/REST 降级/点位归一化/历史累积缓存全套）；manager.bootstrap_sources 移除 ig 注册分支，KQ_MARKET_SOURCES 默认值 binance,ig,mt5→binance,mt5；__init__.py 包说明与 pyproject（ig-streaming 可选依赖）同步清理；.env.example 移除 IG_* 配置块。测试：test_market_sources.py 删除 10 个 IG 专属用例（时间解析/分页拼接/epic 解析/点位归一化/累积缓存），框架级用例中假源名 ig→fx 保持去重/路由语义，全量回归 548 passed。前端无需改动（交易所列表由 /api/market/sources 驱动，外汇已路由 mt5）。网关已重启（PID 4724），sources 仅剩 binance+mt5。未提交 git |
| 2026-08-26 | v9.13 | MACD_MULTI 多倍数指标与参考线用户自选体系（本轮交付）：① 指标移植（TradingView Pine MisterY_MACD_trend）：custom_indicators/macd_multi.py def 式注册，单实例 9 字段（仅 1X 出柱 MCD_1X + 1X/4X/16X/64X 的 DIF/DEA），参数 s=12/p=20/m=9 按倍数 1/4/16/64 展开周期；② dsl.py 契约扩展：style 支持 plot:histogram + hist_colors 四槽（零轴上增/上缩/下增/下缩），新增 price_lines 参考线契约（_validate_price_lines：price 必数值 + 可选 color #RRGGBB/line_style 0~3）随 meta 下发；③ 前端 lc-live.html：柱绘制泛化（isHistField/fieldHistColors，六条渲染路径统一，用户自选 > 后端声明 > 默认色槽），副图默认高度统一 SUB_PANE_H=180，图例半透明背景遮挡修复（透明底 + 文字描边阴影），图例换行修复（.legend 补 right:12px 打破父容器 shrink-to-fit 与 max-width 循环依赖，致末字段掉行的问题），参考线用户自选（样式弹窗参考线区可增删行：数值 + 颜色浮层 + 线型下拉，持久化 styleUI.refLines，优先级 自选 > 默认源，空数组 = 用户明确清空；KD 默认 20/50/80、RSI 70/30），线型图形化 LINE_STYLE_GLYPH（option 直接画线型样本 ──/┈┈/╌╌/┄┄，文字保留 title，线条样式与参考线共用）；④ 策略接入零改动（IND-106 链路原样可用）：strategies/macd_multi.py 示例策略（16X 趋势过滤 + 1X 金叉/死叉触发，大周期柱策略层 2*(DIF_16X-DEA_16X) 同公式还原）+ tests/unit/test_macd_multi_strategy.py 端到端，连同 macd_family/graph 共 19 项通过；⑤ 迭代计划文档状态更新：IND-102/106/109 标 ✅ 完成，IND-101/103 标部分完成（MA/BOLL/KD/RSI 前端待切后端）；⑥ TRIX 补字段级默认色 style。未提交 git |
| 2026-08-26 | 推送记录 | v9.12~v9.13 批量提交并推送（commit 2ed478a，16 文件 +965/-1037）：删除 IG 外汇市场源插件（ig_source/ig_client/注册分支/ig-streaming 依赖/IG_* 配置）+ MACD_MULTI 多倍数指标（9 字段仅 1X 出柱）+ dsl.py 契约扩展（plot:histogram/hist_colors/price_lines）+ 前端柱泛化/副图高度 180/图例遮挡与换行修复/参考线用户自选（KD 20/50/80）/线型图形化 + 示例策略与端到端测试 + 迭代计划 IND 状态更新。标签 v1.6.0-mockup。推送时 git 代理仍指失效的 socks5 10808，临时 -c 覆盖 http://127.0.0.1:7897 成功（全局配置未动）。同时清理工作区 6 个遗留临时诊断脚本 |
| 2026-08-31 | v9.14 | MKT-THS 同花顺 A 股市场源插件落地（填补「国内·股票」空市场，迭代计划 MKT-THS 标 ✅）：① 新增 gateway/market_sources/ths_source.py（约 545 行）——ThsApi 驱动层：threading.Lock 串行（同 MT5 模式）+ 0.03s 限频节流（官方 20ms/次）+ 限频错误重试/断连标记与 30s 冷却重连，全部调用经 asyncio.to_thread；品种编码归一化 _normalize_code（6 位简码→USHA/USZA，13 个市场前缀白名单校验兼容指数代码段含字母如 USHI1A0001）；周期映射 1m/5m/15m/30m/60m→60m、1d→day、1w→week，end_time 翻页走区间查询（start_time+end_time），前复权，A 股精度下限 2 位；② 盘中实时链路 stream_loop：快照轮询按市场前缀分组分批（规避批量限制）→ 当日累计量差分建 m1 桶（A 股收盘时刻惯例：首桶 09:31/尾桶 15:00，午休归上午尾桶、盘后归收盘桶）→ 当日 m1 聚合 5m~1h（按时段网格不串桶），1d 用快照当日累计字段合成，1w 惰性周 K 种子 + 当日增量；首帧快照只建基线不计差分，_seed_today 拉当日 1m 历史填充；非交易时段（工作日 09:15~15:05 外）休眠 30s；③ 注册与配置：manager.bootstrap_sources 增 ths 分支（连接失败跳过不阻塞，默认 KQ_MARKET_SOURCES=binance,mt5,ths），.env/.env.example 增 THS_USERNAME/THS_PASSWORD（缺省游客模式仅限开发），pyproject optional-dependencies 增 `ths = ["thsdk>=1.7"]`；④ 测试：tests/unit/test_ths_source.py 9 用例（编码归一化含指数、m1 标签时段规则、桶网格、klines 转换/精度/翻页、ticker 映射+缓存、目录合并+兑底、盘中差分聚合、周 K 种子+增量），全量回归 562 passed；⑤ 真实账户联调全通：/api/market/sources 出现 ths，1d/1m/1w K 线（茅台 1297.4、6 位简码归一化生效）、ticker（涨跌幅 0.39%）、品种目录 5222 股票 + 580 指数、指标链路（MACD on ths warmed:true）、指数代码 USHI1A0001（上证指数 3952.18）/USZI399300（沪深300 4609.18）复验通过；⑥ 联调修复：初版 _normalize_code 用 s[4:].isdigit() 拒绝指数代码段含字母（USHI1A0001 报 invalid A-share symbol），改前缀白名单校验；⑦ 前端接入（用户追问后补齐）：MarketSource 基类新增 region 字段（global 缺省）随 meta() 下发，ths 声明 region="cn"，/api/market/sources 与 /api/market/symbols 每行携带；lc-live.html 品种搜索弹窗「国内」分段原为硬编码「数据源接入中」占位，改为按行 region 过滤（后端未连回退路径同步带 region），实测目录 cn 5802 / global 7413，新增 region 契约断言 2 条（41 用例全过）。遗留：盘中 WS 实时聚合待交易时段验证；_probe_thsdk*.py 探测脚本（含明文凭证）不入库。未提交 git |
| 2026-08-31 | v9.15 | A 股纯数字展示码（用户习惯需求：国内用户只认 6 位数字代码，要求前后端不乱改先评估方案，经规划模式定案「展示码/路由码分离」后实施）：① 原则：内部链路（REST 参数/WS 主题/curSymbol/localStorage/判重键）继续用 THSCODE 零改动，展示码后端下发前端只渲染（沿用价格精度铁律）；② 后端：ths_source 新增 _display_code（股票/深指数取末 6 位；沪指数探测实测定规则——1B 段 197/202 取「00」+ 末 4 位：上证50 1B0016→000016、科创50 1B0688→000688、沪深300沪 1B0300→000300；无算术规则段走实测静态表：上证指数 1A0001→000001（含目录重复条目 1C0003）、Ａ股/Ｂ股指数 1A0002/0003→000002/000003、行业分类指数 1B0001~0005→000004~000008（实测工业/商业/地产/公用，防与上证指数撞码）；领先指标/创业成交（1C0002/3C0002/3C0003）无交易所码兜底原码段不猜映射）；base.list_symbols 与 /api/market/symbols 每行下发 code 字段（非国内源缺省 = symbol，前端零分支）；③ 前端：lc-live.html 新增 symCode 映射（目录加载时构建，含后端未连回退路径）+ dispOf() 统一入口，搜索弹窗代码列/顶栏品种/图表水印三处改显纯数字（启动时拉目录刷新，未命中兜底原码），搜索关键字追加匹配 code（输 600519/000001 均命中）；④ 撞码处理：000001 同时属平安银行（股票）与上证指数（指数），搜索弹窗双行+类型标签区分由用户选择，后端不做歧义解析；⑤ 验证：新增展示码用例（含行业指数例外/静态表/兜底），42 用例全过；实测 5802 个国内品种 5799 纯 6 位数字，剩 3 为无交易所码的内部编制指数；BTCUSDT/EURUSD 等国外源 code = symbol 不受影响。未提交 git |
| 2026-08-31 | v9.16 | 展示码事故修复 + K 线默认全量（均落地 lc-live.html）：① 事故：用户报“连不上后端”（页面卡占位文案），排查后端健康/接口全 200，浏览器实测控制台 `Cannot access 'symCode' before initialization`——v9.15 把布局恢复块（模块执行期立即运行）里的顶栏/水印改用 dispOf()，而 let symCode 声明在文件靠后，TDZ 异常中断整个脚本，WS/REST 初始化全部未执行；修复：symCode/dispOf/refreshSymLabels 三个定义前移至布局恢复块之前；教训：node --check 只查语法查不出 TDZ，模块顶层立即执行代码引用后文 let/const 必崩，改动后须浏览器实测控制台；② K 线总量默认不限（用户指令：默认显示所有数据，除非用户设置范围）：klineCount 语义改 0=不限（原默认 1000 致日 K 只能回溯到 2022，实测同花顺数据源深度至 2012），懒加载翻页翻到数据源尽头自动停（noMoreData），首屏仍 300 根、每页 1000 根不变（后端单次上限 le=1000 为翻页粒度不动）；右键菜单新增「拉取全部」开关（默认勾选，数字上限输入框联动禁用），取消勾选恢复 100~5000 数字上限；旧布局持久化的默认值 1000 迁移为不限，其余显式设置尊重保留；周期类指标参数上限在不限模式改为跟随实际已加载根数（fieldMax）；WS 增量不再在不限模式下 shift 丢头。冒烟验证页面加载零报错。未提交 git |
| 2026-08-31 | v9.17 | 数据加载约定升级（用户拍板：默认加载所有数据，前端根据屏幕/翻页显示）：lc-live.html 加载链路从“首屏 300 根 + 左滑懒加载”改为“首屏页立即渲染 + 后台自动续载至数据源尽头/用户上限”：① load() 首页拉 PAGE_SIZE=1000 根立即渲染不阻塞，随后 autoPreload 后台循环翻页（每页状态栏进度“加载历史… N 根（全量）”，页间 60ms 防请求风暴）；② 翻页前置逻辑抽取为 pullOlderPage（锚点校验/代次校验防混入，懒加载 loadMore 与预载同路复用）；③ 收尾视窗策略：全量就绪且用户未交互（pointerdown/wheel 监听）则 fitContent 铺满显示全部历史，已交互则保持视窗逻辑坐标平移补偿不跳动；④ 预载硬上限 PRELOAD_HARD_CAP=30000（防分钟级跨年深度卡死），超出部分留左滑懒加载兜底；续载网络异常中止后同样可左滑重试；⑤ 删除 INIT_BARS 常量。浏览器实测：深历史品种进度递增/自动停止/零报错，用户显式上限（1500）被尊重，人工验证通过。未提交 git |
| 2026-08-31 | v9.18 | 高周期与自定义周期（MKT-TF：周/月/季/年四档补齐 + 自定义倍率周期，规划模式定案“后端派生周期层，前端零计算”后实施）：① 现状根因：前端工具栏写死 6 按钮（连周线都没有，尽管三源均原生支持 1w）；月/季/年无任何源原生提供（同花顺最粗到周线）；② 后端新增 gateway/market_sources/derived.py 派生核心（纯函数）：parse_tf 合法性解析（固定档 1M/1Q/1Y + 自定义 2~99 倍率，单位仅 d/w/M，分钟/小时级原生档已覆盖不派生）、bucket_label 桶标签（北京时间开盘时刻：月=当月 1 日/季=季度首日/年=1 月 1 日，Nd 以 1970-01-01、Nw 以 1969-12-29 周一为对齐起点）、aggregate_daily OHLC 合并/量求和（末桶进行中照常输出）、daily_need 日 K 拉取量换算（×2 冗余，封顶 5000）、fetch_derived_klines 统一入口（路由与指标预热共用，end_time 翻页语义不变）；1w 各源原生直供不走派生，月线亦走聚合口径（不接币安原生 1M 避免双口径）；③ 路由拦截：market.py /klines 对派生周期拉日 K 聚合（翻页语义不变，前端预载/懒加载零改动）；indicator_service._fetch_depth 派生档走同款聚合入口（指标预热自动受益）；④ WS 实时：manager 新增派生聚合器 _derived_state（桶累积 + 已完结日量基线/当日累计量分离防重复叠加），派生订阅在 _active_targets 映射为隐式 1d 订阅（源侧零改动），publish_bar 收到日 K 实时 bar 时合成并发布各订阅派生周期最新 bar（指标 on_bar 随之自动驱动）；冷启动/桶翻滚用 REST 拉桶起始日至昨日日 K 预填（防月线最新 bar 开盘价/量基线缺失）；⑤ 元数据：base.meta() 统一下发原生档 + 1w + 1M/1Q/1Y 声明，前端周期按钮不再逐源禁用；⑥ 前端 lc-live.html：工具栏补 周线/月线/季线/年线 + 「自定」入口（输入弹层：格式数字+单位 d/w/M，倍率 2~99，非法红色提示，回车/确定提交）；合法自定义周期动态插入激活按钮（只保留最近使用一个）并进布局持久化；TF_VALID→tfValid 函数（固定档+自定义正则）、tfLabel 统一文案映射（1w=周线/1M=月线/2d=2日…）、日线及以上判定扩为 tfIsDateOnly（含派生四档与自定义）；切所时派生/自定义周期不回退（派生层全源可用）；⑦ 测试：新增 tests/unit/test_derived_tf.py 23 用例（解析合法性/桶标签跨边界北京时区/聚合正确性/换算拉取/假源聚合与翻页），同步更新 meta 契约与 mt5 目录兜底断言（code 字段），全量回归 587 passed；⑧ 实测：REST 派生六档（1M/1Q/1Y/2d/3w/1w）BTCUSDT+茅台桶标签/翻页不重叠/深度继承日 K 全通；WS 端到端实测币安日线实时驱动 1M/1Q 合成（冷启动开盘价与 REST 月线一致、三主题收盘价同步）；浏览器冒烟：11 按钮/月线切换/自定义 2d 插入激活/非法 3h 拦截/强刷布局恢复 6 步全过，控制台零报错（含 mt5 源派生月线）。未提交 git |
| 2026-08-31 | 推送记录 | v9.14~v9.18 批量提交并推送（commit 4d1d983，16 文件 +1561/-76）：MKT-THS 同花顺 A 股插件（驱动层/编码归一化/盘中实时链路）+ A 股纯数字展示码（展示码/路由码分离）+ 展示码 TDZ 事故修复 + K 线默认全量（0=不限 + 右键拉取全部）+ 数据加载约定升级（首屏立即渲染 + 后台续载）+ MKT-TF 高周期与自定义周期（派生周期层 derived.py/路由拦截/指标透传/WS 实时聚合/前端四档+自定弹层）。标签 v1.8.0-mockup。推送时 git 全局代理仍指失效的 socks5 10808，临时 -c 覆盖 http://127.0.0.1:7897（全局配置未动）。_probe_*.py/_parse_gh.py 等探测脚本、截图与 demo 页（含明文凭证类）不入库，留在本地 |
| 2026-08-31 | v9.19 | 品种列表体验 + A 股全史深度修复：① 前端品种列表两行布局（首行中文名+类型标签，次行纯数字展示码+数据源）；② ths 翻页循环重写：原“返回量 < 请求量即判尽头”被 thsdk 隐式单次截断（日线约 6000 根保留最新段）误判，改“非空即续翻、零新增即止”+时间戳去重；窗口完全无数据（越过上市日）由抛错改返回空页供上层判源尽头，裸拉无窗口兜底路径仍抛错；③ binance 源超 1000 根需求按 endTime 内部分页拼接（防派生深分页静默截断），derived 支持 end_time 多批次拉取。实测平安银行日线全量 8483 根自 1991-04-10 完整 |
| 2026-08-31 | v9.20 | 指标显示故障修复（用户复现：调主图参数后主/副图指标全不显示，切周期恢复）：根因是昨日全量加载改造后 klines.length 可达 8000+，而 /api/indicator/history 的 limit 上限 le=5000 直接 422，调参触发 rebuildIndicators 全部实例重拉命中 422 静默空；修复：上限 le=5000→30000（对齐前端 PRELOAD_HARD_CAP）+ indicator_service._MAX_WARMUP_TOTAL=30000 + ensure_warmed 加深重拉走缓存 + 前端 fetchIndSeries limit 钳制 30000 与 !r.ok 显式报错防御 |
| 2026-08-31 | v9.21 | 进程级 K 线缓存与大页续载（用户要求：缓存不入库，进程级即可，同品种同周期来回切换免重复计算）：① 新增 gateway/market_sources/kline_cache.py：键=(源,品种,周期)，OrderedDict LRU 64 条，尾部 3 根刷新保未收盘 bar 不陈旧，锚点向前补页，空页判源尽头，拉取异常退回存量不标尽头，按键 asyncio.Lock 防并发踩踏；原生与派生周期同层缓存（派生命中免重拉日 K 与聚合）；② 接入点：/api/market/klines 路由与指标预热 ensure_warmed 共享同一缓存存量；指标引擎序列容量 10000→30000 防深历史截断；③ 大页续载：用户反馈“缓存没起作用切换仍等”——实测同深命中 0.2~0.3s，真实瓶颈是路由 limit le=1000 强制前端 9 轮翻页；路由上限 le=1000→30000，前端新增 PRELOAD_PAGE_SIZE=5000（后台续载/左滑懒加载大页，首屏仍 1000 保快速首绘）；实测热命中全量 8483 根由 ~2.5s 降至 0.79s；④ 测试：新增 tests/unit/test_kline_cache.py 6 用例（首拉命中零拉取/尾刷/补跑去重/尽头停拉/LRU 淘汰/故障退回），全量 597 passed。缓存随进程，重启即清（设计如此） |
| 2026-08-31 | v9.22 | thsdk 会话劣化防护（用户报“切换品种长时间无反应，一直显示拉取 XX 品种 K 线”）：根因是 thsdk 会话空闲数十分钟后静默劣化（单次调用由 0.1s 退化到数十秒甚至挂死，独立驱动探针新会话 0.04~0.08s 而旧后端同调用 22.6s 为证），叠加前端 fetch 无超时致状态永久停留；修复三件套：① ths_source 驱动调用 8s 超时护栏（ThreadPoolExecutor + result(timeout)，超时丢弃连接强制重连，不调 disconnect 防劣化会话断连调用同样挂死，每次新建执行器防挂死线程堵队列）；② 盘外 60s 心跳保活（小额日 K 拉取防会话空闲休眠，源头预防）；③ 前端 fetchPage AbortSignal.timeout(30s) + 超时专用错误文案（区分后端未连接）。实测护栏生效：8s 超时→0.6s 重连，稳态请求 0.26~1.33s；597 passed。已知：后端暖启动偶发 130s~4min（ths 登录/会话重建慢） |
| 2026-08-31 | v9.23 | 分屏按需加载改造（用户拍板：按需加载为唯一模式，滚动即加载直至数据源尽头，不设拉取全部开关）：移除 autoPreload 后台续载、右键「拉取全部」开关与 klineCount 上限配置，左滑懒加载（loadMore）成为唯一深度加载路径——首屏 PAGE_SIZE=1000 立即渲染，视窗 range.from<10 触发 PRELOAD_PAGE_SIZE=5000 大页拉取，noMoreData 到数据源尽头即停，loadGen 代次防御防混入 |
| 2026-08-31 | v9.24 | 左滑级联拉空全史修复（用户报「右下角显示 1000 根，实际加载了所有历史数据，没有真正的分屏加载」）：后端请求日志实证级联（EURUSD/1h 连续 4 页 limit=5000、end_time 一年一跳）；根因是 pullOlderPage 补根后用逻辑坐标平移补偿还原视窗，但 lightweight-charts 在 setData prepend 后内部左缘重定位使补偿失效，视窗仍贴左缘致 range.from<10 持续成立，loadMore 一页接一页级联直至数据源尽头；修复改时间区间锚定——拉取前记 getVisibleRange() 时间坐标、setData 后 setVisibleRange 精确还原（不依赖逻辑索引，切断级联；与早年小页场景用时间坐标崩坏改逻辑补偿的经验相反，大页场景逻辑补偿失效改回时间锚定，已在代码注释记录实证依据）；状态栏常驻已加载根数便于核验按需深度，loadMore 完成提示「已加载更早历史，共 N 根（已到数据源尽头）」 |
| 2026-08-31 | v9.25 | 左滑提示 WS 断开修复（用户报左滑深页拉取时右下角出现「WS 断开 · 重连中」）：链路是后端事件循环停摆 18~47s（看门狗抓到 7 次 EVENT LOOP BLOCKED，out 日志每次停摆后紧跟 WS disconnected）→ 前端 35s 静默看门狗误判僵尸连接主动断开重连；双元凶（faulthandler 全线程栈实锤）：① asyncio loop.set_debug(True) 使每个 Future/回调执行 traceback.extract_stack + linecache.checkcache 文件 stat，大页高并发时事件循环线程自身被拖垮；② MT5 _to_rows 对上万根深页数据逐行 numpy 标量提取，纯 Python 长循环持 GIL 饿死循环线程（两次 40s+ 停摆均抓到场）；修复：asyncio debug 默认关闭（GATEWAY_ASYNCIO_DEBUG=1 排障重开，看门狗 + faulthandler 保持常开），_to_rows 改列级 astype/tolist 向量化（提速约百倍且不长期持 GIL）；探活深页 5000 根 0.33s，643 passed |
| 2026-08-31 | v9.26 | MT5 子进程隔离根治全进程冻结（用户报页面卡「等待市场源就绪…（3）/WS 连接中…」，后端全进程冻结连每 10s 币安统计都停）：py-spy 全线程栈实锤死锁链——僵尸线程卡在 _mt5.copy_rates_range 的 C 调用内永久挂起且不放 GIL（线程级 8s 超时护栏放它过后本次永久挂起，僵尸线程不可杀），主线程事件循环与全部工作线程排队等 GIL，看门狗的 logger/dump_traceback 也需 GIL 故连栈都 dump 不出；根治：所有 MetaTrader5 包调用移入独立子进程（_mt5_worker + Pipe 协议：(op, payload) 请求 / ("ok"\|"err", 结果) 回发，multiprocessing spawn），父进程超时即 kill 子进程重建，主进程永不冻结；驱动层接口签名不变兼容单测 fake 注入；真实终端冒烟（拉起/1 根/8000 根大页/tick/连发）全过，643 passed |
| 2026-08-31 | v9.27 | THS 登录超时护栏（同会话劣化防护补强）：劣化后重连登录可挂 55s+ 且持全局锁堵死所有请求，connect 加 15s 超时（单工线程池 result(timeout)），超时丢弃重试交给冷却后下次，冷却期内不启新登录防新登录与僵尸登录互踢，僵尸登录线程任其自然退出 |
| 2026-08-31 | 推送记录 | v9.23~v9.27 批量提交并推送（commit 0383a3f，5 文件 +326/-131）：分屏按需加载改造（左滑懒加载为唯一深度加载路径）+ 左滑级联拉空全史修复（时间区间锚定还原视窗）+ 左滑 WS 断开修复（asyncio debug 默认关 + _to_rows 向量化）+ MT5 子进程隔离根治全进程冻结（_mt5_worker + Pipe 协议，超时杀子进程重建）+ THS 登录 15s 超时护栏。标签 v1.9.0-mockup。推送沿用临时 -c 覆盖 http://127.0.0.1:7897（全局代理配置未动）。_chk_*/_probe_* 探测脚本、截图与 demo 页不入库，留在本地 |
| 2026-09-01 | v9.28 | MT5 掉线自愈 + 前端瞬时失败重试（用户报“切品种多了会在某个品种卡住，✕ 未返回 K 线数据，刷新恢复”）：根因链是周末休市窗口终端无响应→护栏强杀子进程后重建持续失败→掉线窗口内所有 MT5 请求返回空；两个放大缺陷（日志/转储实证）：① stream_loop 判 available=False 后睡 60s 死循环永不再重连（日志始终无 reconnected）；② _try_reconnect（含子进程拉起+8s poll）在协程内同步直调堵死事件循环 10s（EVENT LOOP BLOCKED 转储实锤）；修复：不可用分支改每 30s 后台线程（to_thread）持续重连，成功即回正常轮询；前端 load() 拉取空/异常自动重试两次（4s 退避，状态栏“数据源暂不可用，重试中”），覆盖后端自愈窗口免手动刷新；643 passed |
| 2026-09-01 | v9.29 | 外汇价格多显 000 修复（用户报“之前修过又出现”；与 v8.9 IG 浮点噪声同症状不同根因）：根因是 v9.26 子进程隔离埋下的序列化回归——MetaTrader5 包 symbol_info/symbol_info_tick/symbols_get 返回 C 层构造的匿名 namedtuple（__module__=builtins），pickle.dumps 直接 PicklingError，Pipe 回传静默失败父进程收 None（实测复现）→终端 digits 真值丢失→精度退回价格推导被浮点尾差样本污染到 8 位（只增不减缓存）；copy_rates 的 numpy 数组恰好可 pickle 故障被掩盖；修复：子进程回传前 _pipe_safe 统一把 _asdict 对象转普通 dict，消费方（_probe_symbols/_ensure_digits/list_symbols/fetch_ticker）同步改 dict 访问，fake 驱动对齐契约 + 根因回归用例（构造 __module__=builtins 的 namedtuple）；644 passed；13 品种实测下发精度全回终端真值（EURUSD=5/JPY 对=3/XAUUSD=2），tick 真实报价链路一并恢复 |
| 2026-09-01 | v9.30 | 指标自选样式刷新后丢失修复（持久化核查发现）：样式弹窗保存时 styleUI（逐线颜色/线型、MACD 柱四色、参考线）已回写 indLayout 条目随 saveLayout 入库，但刷新恢复映射只拷 type/params 丢弃 styleUI，重建时 buildInstance 读不到→自选样式每次刷新回退默认；修复：加载映射补 styleUI 携带（x.styleUI ?? null）；JS 语法校验通过 |
| 2026-09-01 | 推送记录 | v9.28~v9.30 批量提交并推送（commit a45ffe2，4 文件 +89/-31）：MT5 掉线自愈（不可用分支 30s 后台 to_thread 重连 + 消除同步重连堵死事件循环）+ 前端 load() 瞬时失败自动重试 + 外汇价格多显 000 修复（子进程匿名 namedtuple 不可 pickle 致 digits 丢失，_pipe_safe 转 dict + 消费方改 dict 访问 + 根因回归用例，644 passed）+ 指标自选样式刷新恢复修复。标签 v1.9.1-mockup。推送沿用临时 -c 覆盖 http://127.0.0.1:7897（全局代理配置未动）。_chk_*/_probe_* 探测脚本、截图与 demo 页不入库，留在本地 |
