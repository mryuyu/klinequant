# KlineQuant 更新日志

## v1.0.0-paper (2026-07-29)

**首个正式发布版本：模拟盘 + 回测可用版**（实盘交易能力规划于 v1.1）

### 核心能力

- **行情引擎**：Binance + OKX 双交易所适配器，WebSocket 实时行情，断线自动重连（指数退避），K 线标准化与多周期重采样
- **指标引擎**：MA / EMA / RSI / MACD / BOLL / ATR / KDJ / VWAP 八大指标（polars 自实现，增量计算）
- **信号引擎**：规则引擎（交叉/阈值/比较）+ 组合条件（AND/OR/NOT）+ 信号冷却与路由（AUTO/SEMI/ALERT）
- **交易引擎**：订单状态机 + 持仓管理 + Paper/Backtest 模式，Simulator 模拟撮合
- **风控引擎**：12 条规则（单笔限额/持仓上限/日亏损熔断/频率限制/价格偏离等），fail-closed 原则，支持热更新
- **回测引擎**：撮合模拟 + 3 种滑点模型 + 3 种手续费模型 + 15 项绩效指标 + 参数优化器（网格/随机搜索）
- **策略框架**：TradeClient/MarketClient SDK + 进程沙箱 + 热加载 + 双均线示例策略
- **前端**：Vue 3 + TypeScript + lightweight-charts，10 个页面（行情看板/交易/策略/信号/回测/账户/风控/系统/告警/设置），深色主题，K 线与指标副图同步联动
- **通信层**：ZeroMQ（PUB/SUB + DEALER/REP），msgpack 序列化，Transport 抽象可替换
- **存储层**：DuckDB（10 表 + BatchWriter）+ Redis 缓存
- **安全**：AES-256-GCM 加密存储交易所 API Key，JWT 认证，审计日志
- **运维**：健康检查脚本 + DuckDB 自动备份/恢复 + 优雅退出

### 质量保障

- 单元测试：490 项通过，覆盖率 83%
- E2E 测试：Playwright 18 项
- 混沌测试：ChaosMonkey 故障注入 30 项（网络/API/数据）
- 全链路 DEMO 验证通过：行情 → 指标 → 信号 → 风控 → 下单 → 平仓 → 资金结算

### 已知限制

- **实盘交易未验证**：INT-003 实盘验证延至 v1.1，当前仅 Paper Mode 与 Backtest 经过完整验证
- 仅支持加密货币现货/合约（Binance、OKX），A 股与期货适配在远期路线
- 单机部署（Windows 原生），Docker 编排方案预留至 Linux 环境

### 快速开始

```powershell
# 1. 配置 klinequant/.env（交易所 API Key）
# 2. 一键启动
cd klinequant\scripts
.\start_all.ps1
# 前端: http://127.0.0.1:8080   API: http://127.0.0.1:8000/docs
# 3. 停止
.\stop_all.ps1
```

---

## v0.x (开发阶段，2026-07)

Phase 1 核心闭环 → Phase 2 体验完善（OKX/合约/前端完善/热加载/参数优化/告警）→ Phase 3 核心（高级风控/安全加固/运维监控/混沌测试）。详见《KlineQuant 开发进度跟踪文档》变更记录 v2.0 ~ v7.1。
