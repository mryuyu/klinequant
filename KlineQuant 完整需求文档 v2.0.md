  
**文档版本**：v2.0  
**文档状态**：完整版  
**适用对象**：专业量化交易者 / 小型量化团队（2-8人）  
**文档用途**：作为技术开发文档的输入基准

---

## 1. 项目概述
### 1.1 项目定位
KlineQuant 是一个**面向专业量化交易者的小型团队级量化交易平台**，提供从行情接入、技术指标计算、策略编写、信号生成、风控管理到自动下单的全链路能力。

### 1.2 核心设计原则


| **原则** | **说明** |
| :--- | :--- |
| **计算渲染分离** | 所有计算逻辑在后端完成，前端仅负责数据可视化与交互，确保策略逻辑不暴露于客户端 |
| **通信抽象** | 各模块间通过统一消息协议通信，底层传输可替换（ZMQ / WebSocket / HTTP），为 SaaS 化预留 |
| **故障隔离** | 任一模块崩溃不影响其他模块运行，关键路径具备降级能力 |
| **策略即插件** | 策略以独立进程/容器运行，通过 SDK 与平台交互，支持热加载/热卸载 |
| **三端一致** | 回测、模拟、实盘使用同一套策略代码和信号链路，仅替换执行层 |


### 1.3 技术栈选型（建议）


| **层级** | **技术选型** | **理由** |
| :--- | :--- | :--- |
| 后端核心引擎 | Python 3.10 + / asyncio | 量化生态丰富，asyncio 满足高并发 I/O |
| 高性能计算模块 | Rust / C++ (via PyO3 / pybind11) | 指标计算、撮合引擎的性能瓶颈 |
| 进程间通信 | ZeroMQ (PUB/SUB + REQ/REP) | 低延迟、跨语言、无 broker 依赖 |
| 前端 | Vue 3 + TypeScript + Vite | 轻量、响应式、团队学习成本低 |
| 图表库 | TradingView Lightweight Charts / ECharts | 专业 K 线渲染 |
| 状态管理 | Pinia | Vue 3 官方推荐 |
| 数据库 | DuckDB + ClickHouse + Redis（缓存） | 兼顾关系查询与时序写入性能 |
| 消息队列（SaaS 预留） | Redis Streams / NATS | 轻量级，适合小团队 |
| 容器化 | Docker + Docker Compose | 小团队运维成本最低 |
| CI/CD | GitHub Actions / GitLab CI | 自动化构建与部署 |


### 1.4 项目边界
**包含**：

+ 行情接入与分发（加密货币优先）
+ 技术指标计算引擎
+ 策略运行框架（SDK + 沙箱）
+ 信号生成与路由
+ 风控引擎
+ 交易执行引擎（下单/撤单/查询）
+ 回测引擎
+ Web 前端（行情看板、策略管理、交易面板、回测报告）
+ 本地部署方案

**不包含（远期规划）**：

+ 多租户 SaaS 平台
+ 移动端 App
+ 社交/跟单功能
+ AI 策略生成
+ 合规审计系统（A 股/期货接入时再补充）

---

## 2. 用户画像与使用场景
### 2.1 目标用户


| **用户类型** | **特征** | **核心诉求** |
| :--- | :--- | :--- |
| **独立量化交易者** | 1-2 人，有编程能力（Python），交易加密货币为主 | 快速验证策略想法，7×24 自动运行，不依赖第三方平台 |
| **小型量化团队** | 3-8 人，有分工（策略研究 / 开发 / 运维） | 策略协作管理、统一风控、回测与实盘一致性 |
| **半自动交易者** | 有交易经验，编程能力弱 | 通过可视化配置组合指标，生成信号后手动确认下单 |


### 2.2 核心使用场景
#### 场景 A：策略研发与回测
交易者 Alice 有一个"双均线 + RSI 过滤"的策略想法。她在本地编写策略代码，调用平台 SDK 获取历史 K 线，运行回测，查看夏普比率、最大回撤等绩效报告，调参后再次回测，直到满意。

#### 场景 B：实盘自动交易
Alice 将回测通过的策略部署到实盘环境。系统 7×24 运行，实时接收 Binance 行情，计算指标，生成信号，经风控校验后自动下单。Alice 通过 Web 前端监控持仓和盈亏。

#### 场景 C：多策略并行管理
团队同时运行 5 个策略（趋势跟踪 × 2、均值回归 × 2、套利 × 1），每个策略独立进程运行。团队通过前端统一管理策略生命周期（启动/暂停/停止），设置全局风控（单日最大亏损 5%）。

#### 场景 D：异常处理与人工干预
凌晨 3 点，交易所 API 出现 5 分钟不可用。系统自动暂停相关策略的下单，保持持仓不变，发送告警通知。恢复后自动重连并继续运行。Alice 早上查看告警日志确认无异常。

#### 场景 E：半自动交易
交易者 Bob 不信任全自动下单。他配置策略生成信号后，前端弹出信号卡片（含方向、价格、建议仓位），Bob 点击"确认"后才真正下单。

---

## 3. 系统架构总览
### 3.1 五层架构


```plain
1┌─────────────────────────────────────────────────────────┐
2│                    表现层 (Presentation)                  │
3│         Web Frontend (Vue 3) / CLI / 告警通知            │
4├─────────────────────────────────────────────────────────┤
5│                    接入层 (Gateway)                       │
6│         HTTP API Server / WebSocket Server               │
7├─────────────────────────────────────────────────────────┤
8│                    业务层 (Business)                      │
9│   MarketEngine │ IndicatorEngine │ SignalEngine          │
10│   TradeEngine  │ RiskEngine     │ BacktestEngine        │
11├─────────────────────────────────────────────────────────┤
12│                    策略层 (Strategy)                      │
13│   Strategy Sandbox (独立进程) × N                        │
14│   Strategy SDK (TradeClient / MarketClient)              │
15├─────────────────────────────────────────────────────────┤
16│                    基础设施层 (Infrastructure)            │
17│   DataStorage │ MessageBus(ZMQ) │ Config │ Logger        │
18└─────────────────────────────────────────────────────────┘
```

### 3.2 模块职责


| **模块** | **职责** | **进程模型** |
| :--- | :--- | :--- |
| **MarketEngine** | 接入交易所行情（WS/REST），标准化，分发 K 线/Tick | 独立进程 |
| **IndicatorEngine** | 接收 K 线，计算技术指标（MA/EMA/RSI/MACD/BOLL 等），输出指标值 | 独立进程 |
| **SignalEngine** | 接收指标值，执行信号规则，生成交易信号 | 独立进程 |
| **TradeEngine** | 接收信号，调用风控，执行下单/撤单，管理订单生命周期 | 独立进程 |
| **RiskEngine** | 校验每笔订单是否通过风控规则，维护账户风险状态 | 独立进程（或 TradeEngine 内嵌） |
| **BacktestEngine** | 加载历史数据，模拟撮合，输出绩效报告 | 按需启动 |
| **StrategySandbox** | 运行用户策略代码，通过 SDK 与平台交互 | 每策略一进程 |
| **Gateway** | 对外暴露 HTTP/WS 接口，供前端和外部系统调用 | 独立进程 |
| **DataStorage** | 持久化 K 线、订单、成交、策略配置、日志 | 共享服务 |


### 3.3 数据流


```plain
1交易所 WS/REST
2      │
3      ▼
4 MarketEngine ──(标准化K线)──► IndicatorEngine ──(指标值)──► SignalEngine
5      │                                                          │
6      │                                                    (交易信号)
7      │                                                          │
8      ▼                                                          ▼
9 DataStorage                                              RiskEngine
10 (K线持久化)                                                    │
11                                                          (风控通过)
12                                                               │
13                                                               ▼
14                                                         TradeEngine
15                                                               │
16                                                               ▼
17                                                         交易所 API
18                                                               │
19                                                               ▼
20                                                         DataStorage
21                                                        (订单/成交记录)
```

### 3.4 项目目录结构


```plain
1klinequant/
2├── core/                          # 核心引擎
3│   ├── market_engine/
4│   │   ├── __init__.py
5│   │   ├── engine.py              # 行情引擎主循环
6│   │   ├── adapters/              # 交易所适配器
7│   │   │   ├── base.py            # 适配器抽象基类
8│   │   │   ├── binance.py
9│   │   │   ├── okx.py
10│   │   │   └── ctp.py             # (远期) 期货
11│   │   ├── normalizer.py          # 数据标准化
12│   │   └── timeframe_engine.py    # K线周期重采样
13│   ├── indicator_engine/
14│   │   ├── __init__.py
15│   │   ├── engine.py
16│   │   ├── registry.py            # 指标注册表
17│   │   └── indicators/
18│   │       ├── base.py            # 指标基类
19│   │       ├── ma.py
20│   │       ├── ema.py
21│   │       ├── rsi.py
22│   │       ├── macd.py
23│   │       ├── boll.py
24│   │       └── custom/            # 用户自定义指标
25│   ├── signal_engine/
26│   │   ├── __init__.py
27│   │   ├── engine.py
28│   │   └── rules/
29│   │       ├── base.py
30│   │       ├── crossover.py       # 金叉/死叉
31│   │       ├── threshold.py       # 阈值突破
32│   │       └── composite.py       # 组合条件
33│   ├── trade_engine/
34│   │   ├── __init__.py
35│   │   ├── engine.py
36│   │   ├── order_manager.py       # 订单生命周期管理
37│   │   ├── position_manager.py    # 持仓管理
38│   │   └── executors/
39│   │       ├── base.py
40│   │       ├── binance.py
41│   │       └── simulator.py       # 模拟执行器（回测/模拟盘）
42│   ├── risk_engine/
43│   │   ├── __init__.py
44│   │   ├── engine.py
45│   │   └── rules/
46│   │       ├── base.py
47│   │       ├── position_limit.py
48│   │       ├── loss_limit.py
49│   │       ├── frequency_limit.py
50│   │       └── price_deviation.py
51│   └── backtest_engine/
52│       ├── __init__.py
53│       ├── engine.py
54│       ├── matcher.py             # 撮合模拟器
55│       ├── slippage.py            # 滑点模型
56│       ├── fee.py                 # 手续费模型
57│       └── analytics.py           # 绩效分析
58├── strategy/                      # 策略框架
59│   ├── sdk/
60│   │   ├── __init__.py
61│   │   ├── trade_client.py        # 交易客户端 SDK
62│   │   ├── market_client.py       # 行情客户端 SDK
63│   │   └── context.py             # 策略上下文
64│   ├── sandbox/
65│   │   ├── __init__.py
66│   │   ├── runner.py              # 策略进程管理器
67│   │   └── loader.py              # 策略热加载
68│   └── examples/
69│       ├── dual_ma.py
70│       ├── rsi_reversal.py
71│       └── grid_trading.py
72├── gateway/                       # 接入层
73│   ├── __init__.py
74│   ├── http_server.py             # FastAPI / aiohttp
75│   ├── ws_server.py               # WebSocket 服务
76│   ├── auth.py                    # 认证中间件
77│   └── routers/
78│       ├── market.py
79│       ├── strategy.py
80│       ├── trade.py
81│       ├── backtest.py
82│       └── system.py
83├── storage/                       # 数据层
84│   ├── __init__.py
85│   ├── models.py                  # ORM 模型
86│   ├── repositories/
87│   │   ├── kline_repo.py
88│   │   ├── order_repo.py
89│   │   ├── trade_repo.py
90│   │   └── strategy_repo.py
91│   ├── cache.py                   # Redis 缓存
92│   └── migrations/
93├── protocol/                      # 通信协议
94│   ├── __init__.py
95│   ├── messages.py                # 消息类型定义
96│   ├── types.py                   # 核心数据结构
97│   ├── codec.py                   # 序列化/反序列化
98│   └── transport/
99│       ├── base.py                # 传输抽象
100│       ├── zmq_transport.py
101│       └── ws_transport.py
102├── frontend/                      # 前端
103│   ├── src/
104│   │   ├── views/
105│   │   ├── components/
106│   │   ├── stores/
107│   │   ├── services/
108│   │   └── utils/
109│   └── ...
110├── config/                        # 配置
111│   ├── settings.yaml
112│   ├── exchanges.yaml
113│   └── strategies.yaml
114├── scripts/                       # 运维脚本
115│   ├── start_all.py
116│   ├── stop_all.py
117│   └── health_check.py
118├── tests/
119│   ├── unit/
120│   ├── integration/
121│   └── e2e/
122├── docker/
123│   ├── Dockerfile
124│   └── docker-compose.yaml
125├── docs/
126├── pyproject.toml
127└── README.md
```

---

## 4. 功能需求
### 4.1 行情模块 (MarketEngine)


| **编号** | **功能** | **优先级** | **描述** |
| :--- | :--- | :--- | :--- |
| MKT-001 | 多交易所行情接入 | P0 | 支持 Binance、OKX 的 WebSocket 实时行情推送 |
| MKT-002 | K 线标准化 | P0 | 将各交易所 K 线统一为内部标准格式（见 5.1） |
| MKT-003 | 多周期支持 | P0 | 支持 1m/5m/15m/30m/1h/4h/1d 周期 |
| MKT-004 | 周期重采样 | P0 | 基于 1m K 线实时合成更大周期 K 线 |
| MKT-005 | Tick 数据接收 | P1 | 支持逐笔成交 / 盘口深度数据接收 |
| MKT-006 | 行情断线重连 | P0 | WS 断线后自动重连，重连后补齐缺失 K 线 |
| MKT-007 | 历史数据补全 | P0 | 启动时通过 REST API 拉取缺失的历史 K 线 |
| MKT-008 | 多品种订阅 | P0 | 支持同时订阅多个交易对（≥ 50 个） |
| MKT-009 | 行情数据校验 | P1 | 检测 K 线缺失、时间戳跳跃、价格异常（如 0 值） |
| MKT-010 | 行情快照缓存 | P1 | 维护每个品种最新 K 线的内存快照，供查询 |


### 4.2 指标模块 (IndicatorEngine)


| **编号** | **功能** | **优先级** | **描述** |
| :--- | :--- | :--- | :--- |
| IND-001 | 内置指标库 | P0 | MA、EMA、RSI、MACD、BOLL、ATR、KDJ、VWAP |
| IND-002 | 增量计算 | P0 | 新 K 线到达时仅增量更新指标值，非全量重算 |
| IND-003 | 多周期指标 | P0 | 同一指标可在不同周期上独立计算 |
| IND-004 | 自定义指标 | P1 | 用户通过继承基类注册自定义指标 |
| IND-005 | 指标参数配置 | P0 | 每个指标实例的参数可独立配置（如 MA(7) 和 MA(25)） |
| IND-006 | 指标值订阅 | P0 | 其他模块可订阅特定指标的实时值 |
| IND-007 | 指标预热 | P0 | 启动时加载足够历史数据完成指标初始化（如 MA(200) 需 200 根 K 线） |


### 4.3 信号模块 (SignalEngine)


| **编号** | **功能** | **优先级** | **描述** |
| :--- | :--- | :--- | :--- |
| SIG-001 | 规则引擎 | P0 | 支持基于指标值的条件规则（交叉、阈值、比较） |
| SIG-002 | 组合条件 | P0 | 支持 AND / OR / NOT 逻辑组合 |
| SIG-003 | 信号输出 | P0 | 生成标准化信号（方向、强度、品种、价格、时间） |
| SIG-004 | 信号去重 | P0 | 同一条件在冷却期内不重复触发 |
| SIG-005 | 信号路由 | P0 | 信号可路由到自动下单 / 半自动确认 / 仅告警 |
| SIG-006 | 可视化配置 | P1 | 前端支持拖拽式信号规则配置（无需写代码） |


### 4.4 交易模块 (TradeEngine)


| **编号** | **功能** | **优先级** | **描述** |
| :--- | :--- | :--- | :--- |
| TRD-001 | 下单 | P0 | 支持市价单、限价单 |
| TRD-002 | 撤单 | P0 | 支持按订单 ID 撤单、全部撤单 |
| TRD-003 | 订单查询 | P0 | 查询当前挂单、历史订单、成交记录 |
| TRD-004 | 持仓管理 | P0 | 实时维护各品种持仓数量、均价、未实现盈亏 |
| TRD-005 | 订单状态机 | P0 | 完整状态流转（见 5.3） |
| TRD-006 | 部分成交处理 | P0 | 正确处理部分成交，更新剩余数量 |
| TRD-007 | 止盈止损 | P1 | 支持设置 TP/SL 订单（OCO） |
| TRD-008 | 仓位计算 | P1 | 根据信号强度和风控参数自动计算下单数量 |
| TRD-009 | 交易模式切换 | P0 | 实盘 / 模拟盘 / 回测 三种模式，策略代码不变 |
| TRD-010 | 订单超时 | P1 | 限价单超过 N 秒未成交自动撤单（可配置） |


### 4.5 策略框架 (Strategy)


| **编号** | **功能** | **优先级** | **描述** |
| :--- | :--- | :--- | :--- |
| STR-001 | 策略 SDK | P0 | 提供 TradeClient / MarketClient，策略通过 SDK 交互 |
| STR-002 | 策略生命周期 | P0 | 支持 加载 → 初始化 → 运行 → 暂停 → 停止 → 卸载 |
| STR-003 | 独立进程运行 | P0 | 每个策略在独立进程中运行，崩溃不影响主系统 |
| STR-004 | 热加载 | P1 | 运行中新增/更新策略无需重启系统 |
| STR-005 | 策略参数管理 | P0 | 策略参数可配置、可持久化、可运行时修改 |
| STR-006 | 策略日志 | P0 | 每个策略有独立日志文件，支持日志级别 |
| STR-007 | 策略状态监控 | P0 | 前端可查看每个策略的运行状态、资源占用 |
| STR-008 | 策略模板 | P1 | 提供常见策略模板（双均线、网格、突破等） |


### 4.6 回测模块 (BacktestEngine)


| **编号** | **功能** | **优先级** | **描述** |
| :--- | :--- | :--- | :--- |
| BT-001 | 历史回测 | P0 | 基于历史 K 线数据运行策略 |
| BT-002 | 撮合模拟 | P0 | 模拟订单撮合（见第 10 章） |
| BT-003 | 绩效报告 | P0 | 输出完整绩效指标（见 10.4） |
| BT-004 | 参数优化 | P1 | 支持网格搜索 / 随机搜索参数空间 |
| BT-005 | 多品种回测 | P1 | 支持组合策略的多品种同时回测 |
| BT-006 | 回测结果对比 | P1 | 多次回测结果可视化对比 |
| BT-007 | 回测 vs 实盘一致性 | P0 | 回测与实盘使用同一信号链路和执行接口 |


### 4.7 前端模块


| **编号** | **功能** | **优先级** | **描述** |
| :--- | :--- | :--- | :--- |
| FE-001 | 行情看板 | P0 | 实时 K 线图 + 指标叠加 + 盘口 |
| FE-002 | 策略管理面板 | P0 | 策略列表、启停控制、参数编辑、日志查看 |
| FE-003 | 交易面板 | P0 | 手动下单、当前持仓、挂单列表、成交历史 |
| FE-004 | 信号监控 | P0 | 实时信号流、信号详情、手动确认下单 |
| FE-005 | 回测报告 | P0 | 绩效图表（资金曲线、回撤图）、交易明细表 |
| FE-006 | 风控面板 | P1 | 风控规则配置、风控触发记录 |
| FE-007 | 系统监控 | P1 | 各引擎状态、延迟指标、资源占用 |
| FE-008 | 告警中心 | P1 | 告警列表、告警规则配置、通知渠道管理 |
| FE-009 | 账户总览 | P0 | 总资产、可用资金、当日盈亏、历史盈亏曲线 |


---

## 5. 核心数据结构定义
### 5.1 标准化 K 线 (Kline)


```plain
1@dataclass(frozen=True)
2class Kline:
3    """标准化 K 线数据结构，所有交易所数据统一为此格式"""
4    symbol: str           # 交易对标识，如 "BTC-USDT"
5    exchange: str         # 交易所标识，如 "binance", "okx"
6    timeframe: str        # 周期，如 "1m", "5m", "1h", "1d"
7    timestamp: int        # K 线开盘时间，Unix 毫秒时间戳 (UTC)
8    open: Decimal         # 开盘价，精度由品种决定
9    high: Decimal         # 最高价
10    low: Decimal          # 最低价
11    close: Decimal        # 收盘价
12    volume: Decimal       # 成交量（基础货币数量）
13    quote_volume: Decimal # 成交额（计价货币数量）
14    trade_count: int      # 成交笔数（部分交易所可能为 0）
15    is_closed: bool       # 该 K 线是否已收盘（未收盘为实时快照）
16
17    # 约束
18    # - timestamp 必须对齐到周期边界（如 1m 对齐到分钟）
19    # - high >= max(open, close), low <= min(open, close)
20    # - volume >= 0
21    # - 时区统一为 UTC
```

### 5.2 Tick 数据 (Tick)


```plain
1@dataclass(frozen=True)
2class Tick:
3    """逐笔成交 / 盘口快照"""
4    symbol: str
5    exchange: str
6    timestamp: int        # Unix 毫秒
7    last_price: Decimal   # 最新成交价
8    bid_price: Decimal    # 买一价
9    bid_qty: Decimal      # 买一量
10    ask_price: Decimal    # 卖一价
11    ask_qty: Decimal      # 卖一量
12    volume_24h: Decimal   # 24h 成交量
```

### 5.3 订单 (Order)


```plain
1class OrderSide(Enum):
2    BUY = "BUY"
3    SELL = "SELL"
4
5class OrderType(Enum):
6    MARKET = "MARKET"
7    LIMIT = "LIMIT"
8    STOP_LIMIT = "STOP_LIMIT"
9
10class OrderStatus(Enum):
11    PENDING = "PENDING"           # 已创建，待提交
12    SUBMITTED = "SUBMITTED"       # 已提交到交易所
13    PARTIAL_FILLED = "PARTIAL"    # 部分成交
14    FILLED = "FILLED"             # 完全成交
15    CANCELING = "CANCELING"       # 撤单中
16    CANCELED = "CANCELED"         # 已撤单
17    REJECTED = "REJECTED"         # 被拒绝（风控/交易所）
18    EXPIRED = "EXPIRED"           # 已过期
19    FAILED = "FAILED"             # 提交失败（网络等）
20
21# 状态流转：
22# PENDING → SUBMITTED → PARTIAL_FILLED → FILLED
23#                     → CANCELING → CANCELED
24#                     → REJECTED
25#                     → EXPIRED
26# PENDING → FAILED (网络异常)
27
28@dataclass
29class Order:
30    order_id: str             # 系统内部唯一 ID (UUID)
31    exchange_order_id: str    # 交易所返回的订单 ID
32    strategy_id: str          # 来源策略 ID
33    symbol: str               # 交易对
34    side: OrderSide
35    order_type: OrderType
36    price: Optional[Decimal]  # 限价单价格，市价单为 None
37    quantity: Decimal         # 委托数量
38    filled_quantity: Decimal  # 已成交数量
39    avg_fill_price: Decimal   # 平均成交价
40    status: OrderStatus
41    created_at: int           # 创建时间 (Unix ms)
42    updated_at: int           # 最后更新时间
43    filled_at: Optional[int]  # 完全成交时间
44    cancel_reason: Optional[str]  # 撤单/拒绝原因
45    fee: Decimal              # 手续费
46    fee_currency: str         # 手续费币种
47    client_order_id: str      # 幂等 ID，防重复下单
48    metadata: dict            # 扩展字段（信号来源等）
```

### 5.4 持仓 (Position)


```plain
1@dataclass
2class Position:
3    symbol: str
4    exchange: str
5    side: str               # "LONG" / "SHORT" / "FLAT"
6    quantity: Decimal       # 持仓数量（正数）
7    avg_entry_price: Decimal  # 平均开仓价
8    unrealized_pnl: Decimal   # 未实现盈亏
9    realized_pnl: Decimal     # 已实现盈亏（当日）
10    margin: Decimal           # 占用保证金（合约）
11    leverage: int             # 杠杆倍数（现货为 1）
12    updated_at: int
```

### 5.5 交易信号 (Signal)


```plain
1class SignalDirection(Enum):
2    LONG = "LONG"       # 做多信号
3    SHORT = "SHORT"     # 做空信号
4    CLOSE = "CLOSE"     # 平仓信号
5    NEUTRAL = "NEUTRAL" # 无方向（仅告警）
6
7class SignalStrength(Enum):
8    WEAK = 1
9    MEDIUM = 2
10    STRONG = 3
11
12@dataclass
13class Signal:
14    signal_id: str            # UUID
15    strategy_id: str          # 产生信号的策略
16    symbol: str
17    direction: SignalDirection
18    strength: SignalStrength
19    price: Decimal            # 信号触发时的价格
20    suggested_quantity: Optional[Decimal]  # 建议数量（可为空，由仓位管理计算）
21    reason: str               # 人类可读的触发原因
22    indicators: dict          # 触发时的指标快照 {"MA7": 42000, "RSI14": 72}
23    timestamp: int            # 信号产生时间
24    expires_at: int           # 信号有效期（超过则失效）
25    status: str               # "PENDING" / "CONFIRMED" / "EXECUTED" / "EXPIRED" / "REJECTED"
```

### 5.6 账户 (Account)


```plain
1@dataclass
2class Account:
3    exchange: str
4    account_type: str         # "SPOT" / "FUTURES"
5    total_balance: Decimal    # 总资产（USDT 计价）
6    available_balance: Decimal  # 可用余额
7    frozen_balance: Decimal   # 冻结金额
8    unrealized_pnl: Decimal   # 未实现盈亏
9    positions: List[Position]
10    updated_at: int
```

### 5.7 指标值 (IndicatorValue)


```plain
1@dataclass
2class IndicatorValue:
3    indicator_name: str       # 如 "MA", "RSI", "MACD"
4    symbol: str
5    timeframe: str
6    timestamp: int            # 对应 K 线的 timestamp
7    values: dict              # 如 {"MA": 42000.5} 或 {"MACD": {"DIF": 100, "DEA": 80, "HIST": 20}}
8    params: dict              # 指标参数 {"period": 14}
```

---

## 6. 接口契约（API 定义）
### 6.1 RESTful API（HTTP）
**Base URL**: `http://localhost:8000/api/v1`  
**认证**: Bearer Token (JWT)  
**Content-Type**: `application/json`

#### 6.1.1 行情接口


| **Method** | **Path** | **描述** | **优先级** |
| :--- | :--- | :--- | :--- |
| GET | `/market/klines` | 获取历史 K 线 | P0 |
| GET | `/market/klines/latest` | 获取最新 K 线快照 | P0 |
| GET | `/market/symbols` | 获取支持的品种列表 | P0 |
| GET | `/market/ticker/{symbol}` | 获取品种最新 Ticker | P1 |


**GET /market/klines 参数**：



```plain
1// Request Query Params
2{
3  "symbol": "BTC-USDT",      // 必填
4  "exchange": "binance",     // 必填
5  "timeframe": "1h",         // 必填
6  "start_time": 1700000000000, // 可选，起始时间 (ms)
7  "end_time": 1700086400000,   // 可选，结束时间 (ms)
8  "limit": 500                 // 可选，最大返回数量，默认 200，上限 1000
9}
10
11// Response 200
12{
13  "code": 0,
14  "data": [
15    {
16      "symbol": "BTC-USDT",
17      "timestamp": 1700000000000,
18      "open": "42000.00",
19      "high": "42500.00",
20      "low": "41800.00",
21      "close": "42300.00",
22      "volume": "1234.56",
23      "is_closed": true
24    }
25  ],
26  "total": 500
27}
```

#### 6.1.2 策略接口


| **Method** | **Path** | **描述** | **优先级** |
| :--- | :--- | :--- | :--- |
| GET | `/strategies` | 获取策略列表 | P0 |
| POST | `/strategies` | 注册新策略 | P0 |
| GET | `/strategies/{id}` | 获取策略详情 | P0 |
| PUT | `/strategies/{id}/params` | 更新策略参数 | P0 |
| POST | `/strategies/{id}/start` | 启动策略 | P0 |
| POST | `/strategies/{id}/stop` | 停止策略 | P0 |
| POST | `/strategies/{id}/pause` | 暂停策略 | P1 |
| DELETE | `/strategies/{id}` | 卸载策略 | P1 |
| GET | `/strategies/{id}/logs` | 获取策略日志 | P0 |


#### 6.1.3 交易接口


| **Method** | **Path** | **描述** | **优先级** |
| :--- | :--- | :--- | :--- |
| POST | `/trade/orders` | 创建订单（手动下单） | P0 |
| DELETE | `/trade/orders/{id}` | 撤单 | P0 |
| DELETE | `/trade/orders` | 撤销全部挂单 | P1 |
| GET | `/trade/orders` | 查询订单列表 | P0 |
| GET | `/trade/orders/{id}` | 查询订单详情 | P0 |
| GET | `/trade/positions` | 查询当前持仓 | P0 |
| GET | `/trade/fills` | 查询成交记录 | P0 |
| GET | `/trade/account` | 查询账户信息 | P0 |


**POST /trade/orders 请求体**：



```plain
1{
2  "symbol": "BTC-USDT",
3  "exchange": "binance",
4  "side": "BUY",
5  "type": "LIMIT",
6  "price": "42000.00",
7  "quantity": "0.01",
8  "strategy_id": "str_001",       // 可选，手动下单时为空
9  "client_order_id": "uuid-xxx",  // 幂等 ID
10  "tp_price": "44000.00",        // 可选，止盈价
11  "sl_price": "41000.00"         // 可选，止损价
12}
13
14// Response 201
15{
16  "code": 0,
17  "data": {
18    "order_id": "ord_xxxxx",
19    "status": "SUBMITTED",
20    "exchange_order_id": "123456789"
21  }
22}
23
24// Response 400 (风控拒绝)
25{
26  "code": 40001,
27  "message": "Risk rejected: position limit exceeded",
28  "data": {
29    "rule": "POSITION_LIMIT",
30    "detail": "Current position 10 BTC exceeds limit 5 BTC"
31  }
32}
```

#### 6.1.4 回测接口


| **Method** | **Path** | **描述** | **优先级** |
| :--- | :--- | :--- | :--- |
| POST | `/backtest/run` | 提交回测任务 | P0 |
| GET | `/backtest/tasks` | 获取回测任务列表 | P0 |
| GET | `/backtest/tasks/{id}` | 获取回测结果 | P0 |
| GET | `/backtest/tasks/{id}/trades` | 获取回测交易明细 | P0 |


#### 6.1.5 系统接口


| **Method** | **Path** | **描述** | **优先级** |
| :--- | :--- | :--- | :--- |
| GET | `/system/health` | 健康检查 | P0 |
| GET | `/system/engines/status` | 各引擎运行状态 | P1 |
| GET | `/system/alerts` | 获取告警列表 | P1 |
| PUT | `/system/config` | 更新系统配置 | P1 |


### 6.2 WebSocket API
**Endpoint**: `ws://localhost:8000/ws/v1`  
**认证**: 连接时通过 query param 传递 token：`ws://host/ws/v1?token=xxx`

#### 6.2.1 客户端 → 服务端（订阅）


```plain
1// 订阅 K 线
2{
3  "action": "subscribe",
4  "channel": "kline",
5  "params": {
6    "symbol": "BTC-USDT",
7    "exchange": "binance",
8    "timeframe": "1m"
9  }
10}
11
12// 订阅信号
13{
14  "action": "subscribe",
15  "channel": "signal",
16  "params": {
17    "strategy_id": "str_001"  // 可选，不传则订阅所有
18  }
19}
20
21// 订阅订单更新
22{
23  "action": "subscribe",
24  "channel": "order",
25  "params": {}
26}
27
28// 订阅持仓更新
29{
30  "action": "subscribe",
31  "channel": "position",
32  "params": {}
33}
34
35// 取消订阅
36{
37  "action": "unsubscribe",
38  "channel": "kline",
39  "params": {
40    "symbol": "BTC-USDT",
41    "exchange": "binance",
42    "timeframe": "1m"
43  }
44}
```

#### 6.2.2 服务端 → 客户端（推送）


```plain
1// K 线推送
2{
3  "channel": "kline",
4  "data": {
5    "symbol": "BTC-USDT",
6    "timeframe": "1m",
7    "timestamp": 1700000000000,
8    "open": "42000.00",
9    "high": "42100.00",
10    "low": "41950.00",
11    "close": "42050.00",
12    "volume": "12.34",
13    "is_closed": false
14  }
15}
16
17// 信号推送
18{
19  "channel": "signal",
20  "data": {
21    "signal_id": "sig_xxx",
22    "strategy_id": "str_001",
23    "symbol": "BTC-USDT",
24    "direction": "LONG",
25    "strength": "STRONG",
26    "price": "42050.00",
27    "reason": "MA7 crossed above MA25, RSI14 > 50",
28    "timestamp": 1700000060000
29  }
30}
31
32// 订单更新推送
33{
34  "channel": "order",
35  "data": {
36    "order_id": "ord_xxx",
37    "status": "FILLED",
38    "filled_quantity": "0.01",
39    "avg_fill_price": "42050.00",
40    "updated_at": 1700000061000
41  }
42}
```

#### 6.2.3 心跳机制


```plain
1// 客户端每 30s 发送
2{"action": "ping", "ts": 1700000000000}
3
4// 服务端回复
5{"action": "pong", "ts": 1700000000000}
6
7// 超过 90s 未收到心跳，服务端主动断开连接
```

---

## 7. 通信协议设计
### 7.1 内部消息格式（引擎间通信）
所有引擎间通信使用统一消息信封：



```plain
1@dataclass
2class Message:
3    msg_id: str           # UUID，全局唯一
4    msg_type: str         # 消息类型，如 "KLINE_UPDATE", "ORDER_SUBMIT"
5    source: str           # 发送方模块，如 "market_engine"
6    target: str           # 接收方模块，如 "indicator_engine"，"*" 表示广播
7    timestamp: int        # 发送时间 (Unix ms)
8    payload: dict         # 消息体
9    trace_id: str         # 链路追踪 ID（同一请求链路共享）
10    priority: int         # 优先级 0(低) - 9(高)，默认 5
11
12    def serialize(self) -> bytes:
13        """序列化为 msgpack 二进制"""
14        ...
15
16    @classmethod
17    def deserialize(cls, data: bytes) -> "Message":
18        ...
```

### 7.2 消息类型注册表


| **msg_type** | **方向** | **描述** |
| :--- | :--- | :--- |
| `KLINE_UPDATE` | Market → Indicator / Storage | 新 K 线到达 |
| `KLINE_CLOSED` | Market → Indicator | K 线收盘确认 |
| `TICK_UPDATE` | Market → (订阅者) | Tick 数据 |
| `INDICATOR_UPDATE` | Indicator → Signal / Strategy | 指标值更新 |
| `SIGNAL_GENERATED` | Signal → Trade / Gateway | 新信号产生 |
| `ORDER_SUBMIT` | Trade → Exchange Adapter | 提交订单 |
| `ORDER_UPDATE` | Exchange Adapter → Trade → Gateway | 订单状态变更 |
| `POSITION_UPDATE` | Trade → Gateway / Risk | 持仓变更 |
| `RISK_CHECK` | Trade → Risk | 风控校验请求 |
| `RISK_RESULT` | Risk → Trade | 风控校验结果 |
| `STRATEGY_COMMAND` | Gateway → Strategy Sandbox | 策略控制命令 |
| `STRATEGY_EVENT` | Strategy Sandbox → Gateway | 策略状态上报 |
| `ALERT` | Any → Gateway | 告警事件 |
| `HEARTBEAT` | Any → Monitor | 心跳 |


### 7.3 传输层抽象


```plain
1class Transport(ABC):
2    """传输层抽象，支持替换底层实现"""
3
4    @abstractmethod
5    async def publish(self, topic: str, message: Message) -> None: ...
6
7    @abstractmethod
8    async def subscribe(self, topic: str, handler: Callable) -> None: ...
9
10    @abstractmethod
11    async def request(self, target: str, message: Message, timeout: float = 5.0) -> Message: ...
12
13    @abstractmethod
14    async def start(self) -> None: ...
15
16    @abstractmethod
17    async def stop(self) -> None: ...
18
19
20class ZmqTransport(Transport):
21    """ZeroMQ 实现：PUB/SUB 用于广播，REQ/REP 用于请求响应"""
22    ...
23
24class WebSocketTransport(Transport):
25    """WebSocket 实现：用于 SaaS 模式或跨机器部署"""
26    ...
```

### 7.4 ZMQ 端口规划
| **端口** | **模式** | **用途** |
| :--- | :--- | :--- |
| 5501 | PUB | MarketEngine 行情广播 |
| 5502 | PUB | IndicatorEngine 指标广播 |
| 5503 | PUB | SignalEngine 信号广播 |
| 5504 | PUB | TradeEngine 订单/持仓广播 |
| 5510 | REP | RiskEngine 风控请求响应 |
| 5511 | REP | TradeEngine 交易命令 |
| 5520-5530 | REQ/REP | 策略沙箱通信（每策略一对端口） |


---

## 8. 异常处理与容错机制
### 8.1 错误码体系
| **错误码范围** | **类别** | **示例** |
| :--- | :--- | :--- |
| 10000-19999 | 系统错误 | 10001: 引擎启动失败, 10002: 配置无效 |
| 20000-29999 | 行情错误 | 20001: WS 连接断开, 20002: 数据缺失 |
| 30000-39999 | 交易错误 | 30001: 下单被拒, 30002: 余额不足 |
| 40000-49999 | 风控错误 | 40001: 持仓超限, 40002: 亏损超限 |
| 50000-59999 | 策略错误 | 50001: 策略崩溃, 50002: 策略超时 |
| 60000-69999 | 数据错误 | 60001: 数据库连接失败, 60002: 数据不一致 |


### 8.2 异常分级
| **级别** | **名称** | **处理方式** | **示例** |
| :--- | :--- | :--- | :--- |
| **FATAL** | 致命 | 系统停止，需人工介入 | 数据库不可用、配置损坏 |
| **CRITICAL** | 严重 | 相关模块停止，告警通知 | 交易所 API 密钥失效、行情断线 > 5min |
| **WARNING** | 警告 | 降级运行，记录日志，告警 | 行情断线 < 5min、单次下单失败 |
| **INFO** | 提示 | 仅记录日志 | 策略正常启停、回测完成 |


### 8.3 重试机制
| **场景** | **重试策略** | **最大重试** | **退避算法** |
| :--- | :--- | :--- | :--- |
| 交易所 WS 断线 | 自动重连 | 无限 | 指数退避 1s → 2s → 4s → ... → 60s (上限) |
| REST API 调用失败 | 自动重试 | 3 次 | 指数退避 500ms → 1s → 2s |
| 下单提交失败（网络） | 自动重试 | 2 次 | 固定 1s |
| 下单被交易所拒绝 | **不重试** | 0 | 直接上报 |
| 数据库写入失败 | 自动重试 | 3 次 | 指数退避 100ms → 200ms → 400ms |
| 策略进程崩溃 | 自动重启 | 3 次 / 5min | 固定 5s |


### 8.4 降级方案
| **故障场景** | **降级策略** |
| :--- | :--- |
| 行情源完全不可用 | 暂停所有自动下单，保持现有持仓，前端显示"行情中断"横幅 |
| 交易网关不可用 | 信号正常生成但标记为 PENDING，恢复后按时间顺序补执行（可配置是否补单） |
| 风控引擎不可用 | **拒绝所有新订单**（fail-closed 原则），不允许绕过风控 |
| 数据库不可用 | 行情和交易继续运行（内存模式），数据暂存本地文件，恢复后补写 |
| 单个策略崩溃 | 隔离该策略，不影响其他策略和系统，告警通知 |
| 指标计算超时 | 跳过当前周期，使用上一周期指标值，记录告警 |


### 8.5 订单异常处理
| **异常** | **处理** |
| :--- | :--- |
| 部分成交后交易所断线 | 重连后查询订单状态，同步本地状态 |
| 下单后未收到确认 | 超时（10s）后主动查询，根据结果更新状态 |
| 重复下单（网络重试导致） | 通过 `client_order_id`<br/> 幂等校验，拒绝重复 |
| 撤单失败 | 重试 2 次，仍失败则告警，人工处理 |
| 交易所返回未知状态 | 标记为 UNKNOWN，定时轮询直到状态明确 |


---

## 9. 风控体系
### 9.1 风控架构


```plain
1订单请求 → [Pre-Check 风控] → 通过 → 提交交易所
2                │
3                └→ 拒绝 → 返回拒绝原因 → 告警
```

+ 风控引擎采用 **fail-closed** 原则：风控不可用时拒绝所有订单
+ 风控检查为**同步阻塞**，延迟要求 < 1ms
+ 风控规则支持**热更新**，无需重启

### 9.2 风控规则清单
| **规则 ID** | **名称** | **层级** | **默认值** | **可配置** | **描述** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| RISK-001 | 单笔最大金额 | 订单级 | 10,000 USDT | ✅ | 单笔订单金额不超过阈值 |
| RISK-002 | 单品种最大持仓 | 品种级 | 5 BTC (等值) | ✅ | 单一品种持仓上限 |
| RISK-003 | 总持仓上限 | 账户级 | 100,000 USDT | ✅ | 所有品种总持仓上限 |
| RISK-004 | 单日最大亏损 | 账户级 | 5% 总资产 | ✅ | 当日已实现亏损达到阈值，暂停所有策略 |
| RISK-005 | 单策略最大亏损 | 策略级 | 2% 总资产 | ✅ | 单策略亏损达到阈值，暂停该策略 |
| RISK-006 | 下单频率限制 | 策略级 | 10 次/分钟 | ✅ | 防止策略 bug 导致高频下单 |
| RISK-007 | 价格偏离保护 | 订单级 | ±5% | ✅ | 限价单价格偏离最新价超过阈值则拒绝 |
| RISK-008 | 最小下单量 | 订单级 | 交易所最小值 | ❌ | 不低于交易所要求 |
| RISK-009 | 可用资金检查 | 账户级 | - | ❌ | 下单金额不超过可用余额 |
| RISK-010 | 连续亏损熔断 | 策略级 | 5 次 | ✅ | 连续 N 次亏损后暂停策略 |
| RISK-011 | 夜间/节假日限制 | 全局 | 可配置 | ✅ | 特定时段禁止开仓（仅允许平仓） |
| RISK-012 | 新品种保护 | 全局 | 上市 < 7天 | ✅ | 新上市品种禁止交易 |


### 9.3 风控触发后处理流程


```plain
1风控触发
2  │
3  ├─ 订单级拒绝 → 返回错误码 → 策略收到 REJECTED → 记录日志
4  │
5  ├─ 策略级暂停 → 停止该策略新信号 → 已有持仓保留 → 告警通知
6  │
7  ├─ 账户级熔断 → 停止所有策略 → 可选：全部平仓 → 告警通知 → 需人工解除
8  │
9  └─ 所有触发 → 写入风控日志（不可删除）→ 前端风控面板展示
```

### 9.4 风控日志
每次风控检查（无论通过或拒绝）均记录：



```plain
1@dataclass
2class RiskLog:
3    log_id: str
4    timestamp: int
5    order_id: str
6    strategy_id: str
7    rule_id: str
8    result: str          # "PASS" / "REJECT"
9    detail: str          # 人类可读描述
10    account_snapshot: dict  # 触发时的账户快照
```

---

## 10. 回测引擎
### 10.1 撮合模型
| **订单类型** | **撮合规则** |
| :--- | :--- |
| 市价单 | 以当根 K 线**收盘价**成交（保守估计） |
| 限价买单 | 当根 K 线 `low <= 限价`<br/> 时成交，成交价 = min(限价, open) |
| 限价卖单 | 当根 K 线 `high >= 限价`<br/> 时成交，成交价 = max(限价, open) |
| 止损单 | 触发条件满足后转为市价单，以收盘价成交 |


**约束**：

+ 每根 K 线内最多撮合一次（避免未来函数）
+ 信号产生于 K 线收盘时，下一根 K 线执行（避免 look-ahead bias）
+ 成交量限制：单笔成交量不超过当根 K 线成交量的 10%（可配置）

### 10.2 滑点模型


```plain
1class SlippageModel(ABC):
2    @abstractmethod
3    def calculate(self, order: Order, kline: Kline) -> Decimal: ...
4
5class FixedSlippage(SlippageModel):
6    """固定滑点：价格 ± N 个 tick"""
7    def __init__(self, ticks: int = 1): ...
8
9class PercentageSlippage(SlippageModel):
10    """百分比滑点：价格 ± N%"""
11    def __init__(self, pct: float = 0.001): ...  # 默认 0.1%
12
13class VolumeBasedSlippage(SlippageModel):
14    """基于成交量的动态滑点：订单量 / K线成交量 越大，滑点越大"""
15    def __init__(self, impact_factor: float = 0.1): ...
```

**默认**：使用 `PercentageSlippage(0.001)`，可在回测配置中切换。

### 10.3 手续费模型


```plain
1@dataclass
2class FeeConfig:
3    maker_rate: Decimal    # Maker 费率，如 0.001 (0.1%)
4    taker_rate: Decimal    # Taker 费率，如 0.001
5    fee_currency: str      # 手续费币种，如 "USDT"
6    discount: Decimal      # 折扣（如持有平台币），默认 1.0
7
8# 预设
9BINANCE_SPOT = FeeConfig(maker_rate=Decimal("0.001"), taker_rate=Decimal("0.001"), ...)
10BINANCE_FUTURES = FeeConfig(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005"), ...)
11OKX_SPOT = FeeConfig(maker_rate=Decimal("0.0008"), taker_rate=Decimal("0.001"), ...)
```

### 10.4 绩效指标
回测完成后输出以下指标：

| **指标** | **说明** |
| :--- | :--- |
| 总收益率 | (最终资金 - 初始资金) / 初始资金 |
| 年化收益率 | 按 365 天折算 |
| 夏普比率 (Sharpe) | (年化收益 - 无风险利率) / 年化波动率，无风险利率默认 0 |
| 索提诺比率 (Sortino) | 仅考虑下行波动 |
| 最大回撤 (Max DD) | 峰值到谷值的最大跌幅 |
| 最大回撤持续期 | 从峰值到恢复的最长时间 |
| Calmar 比率 | 年化收益 / 最大回撤 |
| 胜率 | 盈利交易数 / 总交易数 |
| 盈亏比 | 平均盈利 / 平均亏损 |
| 总交易次数 | - |
| 平均持仓时间 | - |
| 最大连续盈利/亏损次数 | - |
| 月度收益分布 | 每月收益率 |
| 资金曲线 | 逐日/逐笔资金变化 |
| 回撤曲线 | 逐日回撤变化 |


### 10.5 回测配置
```plain
1backtest:
2  strategy: "dual_ma"
3  params:
4    fast_period: 7
5    slow_period: 25
6  symbol: "BTC-USDT"
7  exchange: "binance"
8  timeframe: "1h"
9  start_date: "2023-01-01"
10  end_date: "2024-01-01"
11  initial_capital: 100000    # USDT
12  slippage:
13    model: "percentage"
14    value: 0.001
15  fee:
16    model: "binance_spot"
17  position_size:
18    method: "fixed_ratio"    # fixed_amount / fixed_ratio / kelly
19    value: 0.1               # 每次使用 10% 资金
```

---

## 11. 前端需求
### 11.1 页面清单
| **页面** | **路由** | **优先级** | **描述** |
| :--- | :--- | :--- | :--- |
| 行情看板 | `/market` | P0 | K 线图 + 指标 + 品种切换 |
| 交易面板 | `/trade` | P0 | 手动下单 + 持仓 + 挂单 + 成交 |
| 策略管理 | `/strategies` | P0 | 策略列表 + 启停 + 参数 + 日志 |
| 信号监控 | `/signals` | P0 | 实时信号流 + 确认下单 |
| 回测中心 | `/backtest` | P0 | 配置回测 + 查看报告 |
| 账户总览 | `/account` | P0 | 资产 + 盈亏曲线 |
| 风控面板 | `/risk` | P1 | 规则配置 + 触发记录 |
| 系统监控 | `/system` | P1 | 引擎状态 + 告警 |
| 设置 | `/settings` | P1 | 交易所配置 + 通知渠道 |


### 11.2 行情看板详细需求
+ **K 线图**：
    - 支持 1m/5m/15m/30m/1h/4h/1d 周期切换
    - 支持缩放、拖拽、十字光标
    - 支持叠加指标（MA/EMA/BOLL 主图叠加，RSI/MACD/KDJ 副图）
    - 支持画线工具（趋势线、水平线、斐波那契）— P2
    - 实时推送更新（未收盘 K 线实时刷新）
+ **品种选择器**：下拉搜索，显示最新价和涨跌幅
+ **盘口面板**：买卖 5 档（P1）
+ **最新成交**：滚动显示（P2）

### 11.3 交互流程（核心路径）
#### 路径 A：查看行情 → 手动下单
```plain
1打开行情看板 → 选择品种 → 查看 K 线 → 点击"买入/卖出"按钮
2→ 弹出下单面板（价格/数量/类型）→ 确认 → 提交
3→ 交易面板显示新订单 → WS 推送状态更新
```

#### 路径 B：信号确认下单（半自动）
```plain
1信号监控页 → 收到新信号（声音/弹窗提醒）
2→ 查看信号详情（方向/价格/原因/指标快照）
3→ 点击"确认执行" → 弹出下单确认（可修改数量）
4→ 提交 → 订单状态更新
```

#### 路径 C：策略管理
```plain
1策略管理页 → 查看策略列表（状态/盈亏/运行时长）
2→ 点击策略 → 查看详情/参数 → 修改参数 → 保存
3→ 点击"启动"/"停止" → 状态变更 → 日志实时滚动
```

### 11.4 实时数据更新策略
| **数据类型** | **推送频率** | **前端处理** |
| :--- | :--- | :--- |
| K 线（未收盘） | 实时（每次 tick） | 节流 200ms 更新图表 |
| K 线（收盘） | 每周期一次 | 立即更新 |
| 订单状态 | 事件驱动 | 立即更新列表 |
| 持仓 | 事件驱动 + 5s 轮询兜底 | 立即更新 |
| 信号 | 事件驱动 | 立即推送 + 声音提醒 |
| 账户余额 | 10s 轮询 | 更新显示 |


### 11.5 UI/UX 规范
+ **主题**：深色主题为主（交易者习惯），支持浅色切换
+ **配色**：涨 = 绿色 (#0ECB81)，跌 = 红色 (#F6465D)（国际惯例，可配置反转）
+ **字体**：数字使用等宽字体（如 JetBrains Mono）
+ **响应式**：最小支持 1280×720，不要求移动端适配（P2）
+ **国际化**：首期仅中文，预留 i18n 接口

---

## 12. 多市场适配
### 12.1 市场接入优先级
| **阶段** | **市场** | **交易所** | **说明** |
| :--- | :--- | :--- | :--- |
| Phase 1 | 加密货币现货 | Binance | 7×24 交易，API 开放，无资质要求 |
| Phase 2 | 加密货币合约 | Binance Futures | 增加杠杆、做空、资金费率 |
| Phase 3 | A 股 | 券商接口 (QMT/miniQMT) | 需券商资质，T+1，涨跌停 |
| Phase 4 | 期货 | CTP | 需期货账户，保证金制度 |


### 12.2 各市场交易规则差异
| **规则** | **加密现货** | **加密合约** | **A 股** | **期货** |
| :--- | :--- | :--- | :--- | :--- |
| 交易时间 | 7×24 | 7×24 | 9:30-15:00 (UTC+8) | 9:00-15:00 + 夜盘 |
| 最小下单量 | 交易所定义 | 交易所定义 | 100 股 | 1 手 |
| 价格精度 | 品种定义 | 品种定义 | 0.01 元 | 品种定义 |
| 涨跌停 | 无 | 无（有强平） | ±10% / ±20% | ±5%-±20% |
| T+N | T+0 | T+0 | T+1 | T+0 |
| 做空 | 不支持 | 支持 | 融券（受限） | 支持 |
| 杠杆 | 1x | 1-125x | 1x (融资 2x) | 5-20x |
| 结算 | 实时 | 实时 + 资金费率(8h) | 日终 | 日终结算 |


### 12.3 适配器抽象


```plain
1class ExchangeAdapter(ABC):
2    """交易所适配器抽象基类"""
3
4    @abstractmethod
5    async def connect(self) -> None: ...
6
7    @abstractmethod
8    async def disconnect(self) -> None: ...
9
10    @abstractmethod
11    async def subscribe_kline(self, symbol: str, timeframe: str, callback: Callable) -> None: ...
12
13    @abstractmethod
14    async def get_klines(self, symbol: str, timeframe: str, start: int, end: int) -> List[Kline]: ...
15
16    @abstractmethod
17    async def submit_order(self, order: Order) -> str: ...
18
19    @abstractmethod
20    async def cancel_order(self, order_id: str) -> bool: ...
21
22    @abstractmethod
23    async def get_order(self, order_id: str) -> Order: ...
24
25    @abstractmethod
26    async def get_positions(self) -> List[Position]: ...
27
28    @abstractmethod
29    async def get_account(self) -> Account: ...
30
31    @abstractmethod
32    def get_symbol_info(self, symbol: str) -> SymbolInfo: ...
33
34    @abstractmethod
35    def normalize_symbol(self, internal_symbol: str) -> str: ...
36    # 内部 "BTC-USDT" → Binance "BTCUSDT" / OKX "BTC-USDT"
```

### 12.4 时区处理
+ **内部统一使用 UTC 毫秒时间戳**
+ 前端展示时转换为用户本地时区（浏览器自动检测）
+ A 股/期货的交易日划分按交易所本地时间（UTC+8）
+ 所有日志记录 UTC 时间
+ 交易日历 (不同市场交易时段不同)
### 12.5 品种管理


```plain
1@dataclass
2class SymbolInfo:
3    symbol: str             # 内部统一标识 "BTC-USDT"
4    exchange: str
5    base_currency: str      # "BTC"
6    quote_currency: str     # "USDT"
7    price_precision: int    # 价格小数位
8    qty_precision: int      # 数量小数位
9    min_qty: Decimal        # 最小下单量
10    min_notional: Decimal   # 最小下单金额
11    tick_size: Decimal      # 最小价格变动
12    market_type: str        # "SPOT" / "FUTURES" / "SWAP"
13    status: str             # "ACTIVE" / "SUSPENDED" / "DELISTED"
```

品种列表通过配置文件 + 交易所 API 动态同步，支持热更新。

---

## 13. 非功能性需求
### 13.1 性能指标
| **指标** | **目标值** | **测量方式** |
| :--- | :--- | :--- |
| 行情推送延迟（交易所 → 前端） | < 100ms (P99) | 时间戳差值 |
| 指标计算延迟（K线到达 → 指标输出） | < 5ms (P99) | 引擎内部计时 |
| 信号生成延迟（指标 → 信号） | < 2ms (P99) | 引擎内部计时 |
| 风控检查延迟 | < 1ms (P99) | 同步调用计时 |
| 下单延迟（信号 → 交易所确认） | < 200ms (P99) | 时间戳差值 |
| 前端 K 线渲染帧率 | ≥ 30 FPS | 浏览器 Performance API |
| WebSocket 消息吞吐 | ≥ 1000 msg/s | 压测 |
| 回测速度 | ≥ 10,000 K线/s | 回测计时 |


### 13.2 并发能力
| **指标** | **目标值** |
| :--- | :--- |
| 同时运行策略数 | ≥ 20 |
| 同时订阅品种数 | ≥ 100 |
| WebSocket 并发连接数 | ≥ 10（小团队场景） |
| 每秒处理订单数 | ≥ 50 |
| 历史 K 线存储量 | ≥ 5 年 × 100 品种 × 7 周期 |


### 13.3 可用性与可靠性
| **指标** | **目标值** |
| :--- | :--- |
| 系统可用性（交易时段） | ≥ 99.5%（本地部署） |
| 行情断线恢复时间 | < 10s（自动重连） |
| 策略崩溃恢复时间 | < 15s（自动重启） |
| 数据持久化 | 订单/成交数据零丢失 |
| 系统重启后恢复时间 | < 30s（加载配置 + 同步状态） |


### 13.4 可扩展性
+ 新增交易所适配器：无需修改核心引擎代码
+ 新增指标：继承基类 + 注册，无需修改引擎
+ 新增风控规则：继承基类 + 配置启用
+ 传输层替换：实现 Transport 接口即可（ZMQ → WebSocket → gRPC）

### 13.5 资源占用（单机部署基准）
| **资源** | **上限** |
| :--- | :--- |
| CPU | 4 核（正常运行 < 50%） |
| 内存 | 16 GB（正常运行 < 8 GB） |
| 磁盘 | 200 GB（含 10 年历史数据） |
| 网络 | 10 Mbps |


---

## 14. 安全性设计
### 14.1 认证与授权
| **项目** | **方案** |
| :--- | :--- |
| 认证方式 | JWT (JSON Web Token)，有效期 24h，支持刷新 |
| 初始部署 | 首次启动生成管理员账号，强制修改默认密码 |
| 权限模型 | 单用户模式（小团队）：所有权限；多用户模式（预留）：RBAC |
| API 密钥管理 | 交易所 API Key/Secret 使用 AES-256 加密存储，密钥由主密码派生 |


### 14.2 通信安全
| **场景** | **方案** |
| :--- | :--- |
| 前端 ↔ 后端 (HTTP) | 生产环境强制 HTTPS (TLS 1.3) |
| 前端 ↔ 后端 (WS) | WSS (TLS) |
| 引擎间 (ZMQ) | 本地部署：localhost 绑定，无需加密；跨机器：CurveZMQ 加密 |
| 策略沙箱 ↔ 主系统 | 本地 IPC，限制网络访问 |


### 14.3 安全防护
| **威胁** | **防护措施** |
| :--- | :--- |
| XSS | 前端输入转义，CSP 头 |
| CSRF | SameSite Cookie + Token 校验 |
| 重放攻击 | request_id + timestamp + 5min 有效期 |
| 暴力破解 | 登录失败 5 次锁定 15min |
| API Key 泄露 | 加密存储，内存中仅在使用时解密，日志脱敏 |
| 策略代码恶意行为 | 沙箱隔离，限制文件系统/网络访问，资源配额 |


### 14.4 审计日志
所有关键操作记录不可篡改的审计日志：

+ 登录/登出
+ 下单/撤单
+ 策略启停
+ 风控规则变更
+ 配置修改
+ API Key 操作

---

## 15. 数据存储设计
### 15.1 存储分层
| **数据类型** | **存储介质** | **保留策略** |
| :--- | :--- | :--- |
| 实时 K 线（当前周期） | Redis / 内存 | 最新 1000 根 |
| 历史 K 线 | TimescaleDB | 永久（可配置归档） |
| 订单/成交记录 | PostgreSQL | 永久 |
| 策略配置 | PostgreSQL + YAML 文件 | 永久 |
| 风控日志 | PostgreSQL | 1 年 |
| 系统日志 | 文件 (按日轮转) | 30 天 |
| 回测结果 | PostgreSQL + 文件 | 90 天（可手动保留） |
| 缓存（Ticker/余额） | Redis | TTL 自动过期 |


### 15.2 关键表结构
```plain
1-- K 线表（TimescaleDB 超表）
2CREATE TABLE klines (
3    time        TIMESTAMPTZ NOT NULL,
4    symbol      TEXT NOT NULL,
5    exchange    TEXT NOT NULL,
6    timeframe   TEXT NOT NULL,
7    open        NUMERIC(20,8) NOT NULL,
8    high        NUMERIC(20,8) NOT NULL,
9    low         NUMERIC(20,8) NOT NULL,
10    close       NUMERIC(20,8) NOT NULL,
11    volume      NUMERIC(20,8) NOT NULL,
12    quote_vol   NUMERIC(20,8),
13    trade_count INTEGER,
14    PRIMARY KEY (time, symbol, exchange, timeframe)
15);
16SELECT create_hypertable('klines', 'time');
17
18-- 订单表
19CREATE TABLE orders (
20    order_id        UUID PRIMARY KEY,
21    exchange_oid    TEXT,
22    strategy_id     TEXT,
23    symbol          TEXT NOT NULL,
24    exchange        TEXT NOT NULL,
25    side            TEXT NOT NULL,
26    order_type      TEXT NOT NULL,
27    price           NUMERIC(20,8),
28    quantity        NUMERIC(20,8) NOT NULL,
29    filled_qty      NUMERIC(20,8) DEFAULT 0,
30    avg_fill_price  NUMERIC(20,8),
31    status          TEXT NOT NULL,
32    fee             NUMERIC(20,8) DEFAULT 0,
33    fee_currency    TEXT,
34    client_oid      TEXT UNIQUE,
35    created_at      TIMESTAMPTZ NOT NULL,
36    updated_at      TIMESTAMPTZ NOT NULL,
37    metadata        JSONB
38);
39
40-- 策略配置表
41CREATE TABLE strategies (
42    strategy_id   TEXT PRIMARY KEY,
43    name          TEXT NOT NULL,
44    module_path   TEXT NOT NULL,
45    params        JSONB NOT NULL DEFAULT '{}',
46    status        TEXT NOT NULL DEFAULT 'STOPPED',
47    created_at    TIMESTAMPTZ NOT NULL,
48    updated_at    TIMESTAMPTZ NOT NULL
49);
```

### 15.3 数据备份
+ PostgreSQL：每日全量备份 + WAL 归档（增量）
+ 配置文件：Git 版本管理
+ 备份保留：最近 7 天日备份 + 最近 4 周周备份

---

## 16. 测试策略
### 16.1 测试分层
| **层级** | **覆盖目标** | **工具** | **覆盖率要求** |
| :--- | :--- | :--- | :--- |
| 单元测试 | 指标计算、风控规则、数据标准化、订单状态机 | pytest | 核心模块 ≥ 90% |
| 集成测试 | 引擎间通信、行情→指标→信号→下单全链路 | pytest + testcontainers | 关键路径 100% |
| 回测验证 | 已知策略在已知数据上的结果一致性 | 自定义断言 | 每次发版必跑 |
| 性能测试 | 延迟、吞吐量 | locust / 自定义脚本 | 满足 13.1 指标 |
| 前端 E2E | 核心用户路径 | Playwright | 5 条核心路径 |
| 混沌测试 | 断网、进程崩溃、数据异常 | 手动 + 脚本 | 每阶段一次 |


### 16.2 关键测试用例
| **用例** | **验证点** |
| :--- | :--- |
| 双均线策略回测 | 与手动计算结果一致（误差 < 0.01%） |
| 行情断线重连 | 断线 5s 后重连，K 线无缺失 |
| 风控拒绝 | 超限时订单被拒，返回正确错误码 |
| 策略崩溃隔离 | 策略 A 崩溃，策略 B 和主系统不受影响 |
| 部分成交 | 订单状态正确流转，持仓数量正确 |
| 并发下单 | 10 个策略同时下单，无竞态条件 |
| 幂等性 | 相同 client_order_id 重复提交，仅执行一次 |
| 回测 vs 实盘 | 同一策略同一数据，信号完全一致 |


### 16.3 验收标准（Definition of Done）
每个功能交付需满足：

+ 代码通过 Code Review
+ 单元测试通过且覆盖率达标
+ 集成测试通过
+ 无 P0/P1 级已知 Bug
+ 文档更新（API 文档 / 用户手册）
+ 性能指标满足要求

---

## 17. 部署与运维
### 17.1 部署架构（本地/小团队）
```plain
1┌─────────────────────────────────────────────┐
2│              单机 / 小型服务器                │
3│                                             │
4│  ┌─────────┐  ┌─────────┐  ┌─────────┐    │
5│  │ Market  │  │Indicator│  │ Signal  │    │
6│  │ Engine  │  │ Engine  │  │ Engine  │    │
7│  └────┬────┘  └────┬────┘  └────┬────┘    │
8│       │             │             │         │
9│       └──────── ZMQ Bus ─────────┘         │
10│                     │                       │
11│  ┌─────────┐  ┌────┴────┐  ┌─────────┐    │
12│  │  Trade  │  │  Risk   │  │ Gateway │    │
13│  │ Engine  │  │ Engine  │  │(FastAPI)│    │
14│  └─────────┘  └─────────┘  └────┬────┘    │
15│                                  │         │
16│  ┌─────────┐  ┌─────────┐      │         │
17│  │PostgreSQL│  │  Redis  │      │         │
18│  │+Timescale│  │         │      │         │
19│  └─────────┘  └─────────┘      │         │
20│                                  │         │
21│  ┌──────────────────────────────┐│         │
22│  │   Nginx (反向代理 + 静态)    ││         │
23│  └──────────────┬───────────────┘│         │
24│                 │                │         │
25│  ┌──────────────┴───────────────┐│         │
26│  │   Vue Frontend (静态文件)    ││         │
27│  └──────────────────────────────┘│         │
28└─────────────────────────────────────────────┘
```

### 17.2 Docker Compose 服务编排（仅Linux环境，windows环境下不使用）
```plain
1services:
2  market-engine:
3    build: ./core/market_engine
4    restart: unless-stopped
5    network_mode: host  # ZMQ 需要
6
7  indicator-engine:
8    build: ./core/indicator_engine
9    restart: unless-stopped
10
11  signal-engine:
12    build: ./core/signal_engine
13    restart: unless-stopped
14
15  trade-engine:
16    build: ./core/trade_engine
17    restart: unless-stopped
18
19  risk-engine:
20    build: ./core/risk_engine
21    restart: unless-stopped
22
23  gateway:
24    build: ./gateway
25    ports:
26      - "8000:8000"
27    restart: unless-stopped
28
29  frontend:
30    build: ./frontend
31    ports:
32      - "80:80"
33    restart: unless-stopped
34
35  postgres:
36    image: timescale/timescaledb:latest-pg15
37    volumes:
38      - pgdata:/var/lib/postgresql/data
39    restart: unless-stopped
40
41  redis:
42    image: redis:7-alpine
43    restart: unless-stopped
44
45volumes:
46  pgdata:
```

### 17.3 监控与告警
| **监控项** | **工具** | **告警阈值** |
| :--- | :--- | :--- |
| 引擎进程存活 | Docker health check / supervisord | 进程退出即告警 |
| 行情延迟 | 自定义指标 (Prometheus) | > 500ms |
| 下单延迟 | 自定义指标 | > 1s |
| CPU / 内存 | node_exporter + Grafana | CPU > 80%, Mem > 85% |
| 磁盘使用 | node_exporter | > 90% |
| 策略异常 | 日志关键字 | ERROR 级别日志 |
| 风控触发 | 风控日志 | 任何 REJECT |


**告警通知渠道**（按优先级）：

1. 前端弹窗 + 声音
2. 日志记录
3. Webhook（钉钉/飞书/Telegram）— P1
4. 邮件 — P2

### 17.4 日志规范
```plain
1# 日志格式
2"%(asctime)s | %(levelname)-7s | %(name)s | %(trace_id)s | %(message)s"
3
4# 示例
5# 2024-01-15 08:30:01.123 | INFO    | trade_engine | abc-123 | Order submitted: ord_xxx BTC-USDT BUY 0.01 @ 42000
6
7# 日志级别使用
8# DEBUG: 开发调试（生产关闭）
9# INFO:  正常业务流转（订单提交、策略启停）
10# WARNING: 可恢复异常（重连、重试）
11# ERROR: 不可恢复异常（下单失败、策略崩溃）
12# CRITICAL: 系统级故障（数据库不可用）
```

### 17.5 升级与回滚
+ 使用 Docker 镜像标签管理版本
+ 升级前自动备份数据库
+ 支持一键回滚到上一版本
+ 数据库 Migration 使用 Alembic，支持 upgrade/downgrade

---

## 18. 项目管理与路线图
### 18.1 阶段规划
#### Phase 1：核心闭环（预估 8-10 周）
**目标**：实现加密货币现货的"行情 → 指标 → 信号 → 下单"全链路 + 回测

| **周次** | **里程碑** | **交付物** |
| :--- | :--- | :--- |
| W1-2 | 基础设施 | 项目骨架、通信协议、数据结构、CI 搭建 |
| W3-4 | 行情引擎 | Binance 适配器、K 线标准化、WS 推送、断线重连 |
| W5-6 | 指标 + 信号 | 内置指标库、信号规则引擎、全链路联调 |
| W7-8 | 交易引擎 | 下单/撤单、订单状态机、持仓管理、风控基础规则 |
| W9 | 回测引擎 | 撮合模拟、绩效报告、一致性验证 |
| W10 | 前端 MVP | K 线图、交易面板、策略管理（基础） |


**Phase 1 验收标准**：

+ 双均线策略可在 Binance 实盘自动运行
+ 回测结果与手动计算一致
+ 行情断线 10s 内自动恢复
+ 风控规则生效

#### Phase 2：体验完善（预估 6-8 周）
| **内容** | **说明** |
| :--- | :--- |
| OKX 适配器 | 第二交易所接入 |
| 合约支持 | 做空、杠杆、资金费率 |
| 前端完善 | 信号监控、回测报告可视化、风控面板 |
| 策略框架增强 | 热加载、多策略并行、参数优化 |
| 告警通知 | Webhook 集成 |
| 性能优化 | 指标增量计算优化、前端渲染优化 |


#### Phase 3：扩展与稳定（预估 6-8 周）
| **内容** | **说明** |
| :--- | :--- |
| A 股 / 期货适配 | QMT / CTP 接入（需资质） |
| 高级风控 | 多层级风控、动态规则 |
| 运维增强 | 监控大盘、自动备份、健康检查 |
| SaaS 预留 | 传输层切换验证、多用户基础 |
| 文档完善 | 用户手册、策略开发指南、API 文档 |


### 18.2 风险评估
| **风险** | **概率** | **影响** | **缓解措施** |
| :--- | :--- | :--- | :--- |
| 交易所 API 变更 | 中 | 高 | 适配器隔离，关注官方公告，预留适配时间 |
| 行情数据质量问题 | 中 | 中 | 数据校验 + 多源对比 |
| 策略 bug 导致亏损 | 高 | 高 | 风控兜底 + 模拟盘验证 + 小资金试跑 |
| 团队人员变动 | 中 | 中 | 代码规范 + 文档完善 + 模块化降低耦合 |
| 性能瓶颈 | 低 | 中 | 早期压测，预留 Rust 重写路径 |
| 合规风险（A股/期货） | 中 | 高 | Phase 3 再介入，提前了解资质要求 |


### 18.3 团队分工建议（3-5 人团队）
| **角色** | **人数** | **职责** |
| :--- | :--- | :--- |
| 后端 / 引擎开发 | 1-2 | 核心引擎、通信、交易、风控 |
| 策略框架 / 回测 | 1 | SDK、沙箱、回测引擎 |
| 前端开发 | 1 | Web 前端全部页面 |
| 全栈 / 运维 | 1 | 部署、监控、测试、文档 |


---

## 19. 术语表
| **术语** | **全称 / 说明** |
| :--- | :--- |
| Kline | K 线，即 OHLCV 蜡烛图数据 |
| Tick | 逐笔成交 / 最小行情单位 |
| Timeframe | K 线周期（1m, 5m, 1h 等） |
| IPC | Inter-Process Communication，进程间通信 |
| ZMQ | ZeroMQ，高性能消息队列库 |
| SaaS | Software as a Service，软件即服务 |
| CTP | 中国期货市场的交易接口协议 |
| QMT | 迅投量化交易终端（A 股） |
| DIF/DEA | MACD 指标的快线/慢线 |
| OCO | One-Cancels-the-Other，止盈止损组合订单 |
| Maker/Taker | 挂单方（提供流动性）/ 吃单方（消耗流动性） |
| Slippage | 滑点，预期价格与实际成交价格的偏差 |
| Sharpe Ratio | 夏普比率，风险调整后收益指标 |
| Max Drawdown | 最大回撤，峰值到谷值的最大跌幅 |
| Fail-closed | 故障时默认拒绝（安全优先） |
| Fail-open | 故障时默认放行（可用性优先） |
| RBAC | Role-Based Access Control，基于角色的访问控制 |
| WAL | Write-Ahead Logging，数据库预写日志 |
| CSP | Content Security Policy，内容安全策略 |


---

## 20. 附录
### 20.1 策略 SDK 完整接口
```plain
1class TradeClient:
2    """策略交易客户端 SDK"""
3
4    def __init__(self, strategy_id: str, transport: Transport): ...
5
6    # ─── 下单 ───
7    async def market_buy(self, symbol: str, quantity: Decimal, **kwargs) -> Order: ...
8    async def market_sell(self, symbol: str, quantity: Decimal, **kwargs) -> Order: ...
9    async def limit_buy(self, symbol: str, price: Decimal, quantity: Decimal, **kwargs) -> Order: ...
10    async def limit_sell(self, symbol: str, price: Decimal, quantity: Decimal, **kwargs) -> Order: ...
11
12    # ─── 撤单 ───
13    async def cancel_order(self, order_id: str) -> bool: ...
14    async def cancel_all(self, symbol: Optional[str] = None) -> int: ...
15
16    # ─── 查询 ───
17    async def get_order(self, order_id: str) -> Order: ...
18    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]: ...
19    async def get_positions(self) -> List[Position]: ...
20    async def get_position(self, symbol: str) -> Optional[Position]: ...
21    async def get_account(self) -> Account: ...
22    async def get_fills(self, symbol: Optional[str] = None, limit: int = 100) -> List[Fill]: ...
23
24    # ─── 止盈止损 ───
25    async def set_tp_sl(self, symbol: str, tp_price: Decimal, sl_price: Decimal) -> None: ...
26
27
28class MarketClient:
29    """策略行情客户端 SDK"""
30
31    def __init__(self, strategy_id: str, transport: Transport): ...
32
33    # ─── 历史数据 ───
34    async def get_klines(self, symbol: str, timeframe: str, limit: int = 200) -> List[Kline]: ...
35    async def get_klines_range(self, symbol: str, timeframe: str, start: int, end: int) -> List[Kline]: ...
36
37    # ─── 实时订阅 ───
38    async def subscribe_kline(self, symbol: str, timeframe: str, callback: Callable[[Kline], None]) -> None: ...
39    async def unsubscribe_kline(self, symbol: str, timeframe: str) -> None: ...
40
41    # ─── 指标查询 ───
42    async def get_indicator(self, name: str, symbol: str, timeframe: str, limit: int = 1) -> List[IndicatorValue]: ...
43
44    # ─── 最新价 ───
45    async def get_last_price(self, symbol: str) -> Decimal: ...
46
47
48class StrategyContext:
49    """策略上下文，注入到策略实例中"""
50
51    trade: TradeClient
52    market: MarketClient
53    params: dict              # 策略参数
54    logger: logging.Logger    # 策略专属 logger
55    state: dict               # 策略持久化状态（自动保存/恢复）
56
57    def save_state(self) -> None: ...
58    def load_state(self) -> None: ...
```

### 20.2 策略代码示例


```plain
1"""双均线交叉策略"""
2from strategy.sdk import StrategyContext, Kline
3from decimal import Decimal
4
5
6class DualMAStrategy:
7    """
8    参数:
9        fast_period: 快线周期 (默认 7)
10        slow_period: 慢线周期 (默认 25)
11        trade_symbol: 交易品种 (默认 BTC-USDT)
12        timeframe: K线周期 (默认 1h)
13        position_ratio: 每次开仓使用资金比例 (默认 0.1)
14    """
15
16    def __init__(self, ctx: StrategyContext):
17        self.ctx = ctx
18        self.fast = ctx.params.get("fast_period", 7)
19        self.slow = ctx.params.get("slow_period", 25)
20        self.symbol = ctx.params.get("trade_symbol", "BTC-USDT")
21        self.timeframe = ctx.params.get("timeframe", "1h")
22        self.ratio = Decimal(str(ctx.params.get("position_ratio", 0.1)))
23        self.in_position = False
24
25    async def on_init(self):
26        """策略初始化：预热指标"""
27        self.ctx.logger.info(f"DualMA initialized: MA({self.fast}/{self.slow}) on {self.symbol}")
28        # 加载历史数据预热
29        klines = await self.ctx.market.get_klines(self.symbol, self.timeframe, limit=self.slow + 50)
30        self.ctx.logger.info(f"Loaded {len(klines)} klines for warmup")
31
32    async def on_kline(self, kline: Kline):
33        """每根 K 线收盘时触发"""
34        if not kline.is_closed:
35            return
36
37        # 获取指标值
38        ma_fast = await self.ctx.market.get_indicator("MA", self.symbol, self.timeframe, limit=2)
39        ma_slow = await self.ctx.market.get_indicator("MA", self.symbol, self.timeframe, limit=2)
40
41        if len(ma_fast) < 2 or len(ma_slow) < 2:
42            return
43
44        fast_prev = ma_fast[0].values["MA"]
45        fast_curr = ma_fast[1].values["MA"]
46        slow_prev = ma_slow[0].values["MA"]
47        slow_curr = ma_slow[1].values["MA"]
48
49        # 金叉：快线从下方穿越慢线
50        if fast_prev <= slow_prev and fast_curr > slow_curr and not self.in_position:
51            account = await self.ctx.trade.get_account()
52            qty = (account.available_balance * self.ratio) / kline.close
53            await self.ctx.trade.market_buy(self.symbol, qty)
54            self.in_position = True
55            self.ctx.logger.info(f"GOLDEN CROSS: BUY {qty:.6f} {self.symbol} @ {kline.close}")
56
57        # 死叉：快线从上方穿越慢线
58        elif fast_prev >= slow_prev and fast_curr < slow_curr and self.in_position:
59            pos = await self.ctx.trade.get_position(self.symbol)
60            if pos and pos.quantity > 0:
61                await self.ctx.trade.market_sell(self.symbol, pos.quantity)
62            self.in_position = False
63            self.ctx.logger.info(f"DEATH CROSS: SELL {self.symbol} @ {kline.close}")
64
65    async def on_stop(self):
66        """策略停止时清理"""
67        self.ctx.logger.info("DualMA strategy stopped")
```

### 20.3 配置文件示例
```plain
1# config/settings.yaml
2system:
3  name: "KlineQuant"
4  mode: "live"              # live / paper / backtest
5  log_level: "INFO"
6  timezone: "UTC"
7
8gateway:
9  host: "0.0.0.0"
10  port: 8000
11  ws_port: 8001
12  jwt_secret: "${JWT_SECRET}"  # 环境变量注入
13  cors_origins:
14    - "http://localhost:3000"
15
16storage:
17  postgres:
18    host: "localhost"
19    port: 5432
20    database: "klinequant"
21    user: "${DB_USER}"
22    password: "${DB_PASSWORD}"
23  redis:
24    host: "localhost"
25    port: 6379
26    db: 0
27
28transport:
29  type: "zmq"               # zmq / websocket
30  zmq:
31    pub_base_port: 5501
32    rep_base_port: 5510
33
34risk:
35  rules:
36    - id: "RISK-001"
37      enabled: true
38      params:
39        max_order_amount: 10000
40    - id: "RISK-004"
41      enabled: true
42      params:
43        max_daily_loss_pct: 0.05
44    - id: "RISK-006"
45      enabled: true
46      params:
47        max_orders_per_minute: 10
48
49alerts:
50  channels:
51    - type: "webhook"
52      url: "${ALERT_WEBHOOK_URL}"
53      enabled: true
54    - type: "log"
55      enabled: true
```

### 20.4 文档变更记录


| **版本** | **日期** | **变更内容** | **作者** |
| :--- | :--- | :--- | :--- |
| v1.0 | - | 初始架构概念设计 | - |
| v2.0 | - | 完整版：补充数据结构、API 契约、异常处理、风控、回测、非功能性需求、测试、部署等 | - |


**文档结束**  


<font style="color:rgb(0, 0, 0);">  
</font>

