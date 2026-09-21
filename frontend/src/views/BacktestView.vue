<template>
  <div>
    <h2>回测报告</h2>

    <!-- 引擎切换 -->
    <div class="engine-tabs">
      <button class="tab" :class="{ active: engine === 'fx' }" @click="engine = 'fx'">FX 同构回测</button>
      <button class="tab" :class="{ active: engine === 'binance' }" @click="engine = 'binance'">币安 dual_ma</button>
    </div>

    <!-- ============ FX 同构回测 ============ -->
    <div v-if="engine === 'fx'">
      <div class="card run-bar">
        <div class="field">
          <label>品种（逗号分隔）</label>
          <input v-model="fxForm.symbols" class="input" placeholder="如 EURUSD,USDJPY" />
        </div>
        <div class="field">
          <label>周期</label>
          <select v-model="fxForm.period" class="input">
            <option v-for="tf in ['1m','5m','15m','30m','1h','4h','1d']" :key="tf" :value="tf">{{ tf }}</option>
          </select>
        </div>
        <div class="field">
          <label>策略</label>
          <select v-model="fxForm.strategy" class="input">
            <option v-for="s in fxStrategies" :key="s" :value="s">{{ s }}</option>
          </select>
        </div>
        <div class="field">
          <label>K线根数</label>
          <input v-model.number="fxForm.bars" class="input input-sm" type="number" placeholder="建议≥100" />
        </div>
        <div class="field">
          <label>初始资金</label>
          <input v-model.number="fxForm.capital" class="input input-sm" type="number" />
        </div>
        <button class="btn btn-primary" @click="runFx" :disabled="fxRunning">{{ fxRunning ? '回测中...' : '提交回测' }}</button>
      </div>

      <!-- 多品种切换 -->
      <div v-if="fxSymbols.length > 1" class="toolbar">
        <select v-model="fxSelectedSym" class="input">
          <option v-for="sym in fxSymbols" :key="sym" :value="sym">{{ sym }}</option>
        </select>
        <span class="text-muted">共 {{ fxSymbols.length }} 品种</span>
      </div>

      <div v-if="fxReport" class="report">
        <div class="metrics-grid">
          <div class="metric-card">
            <div class="metric-label">总收益率</div>
            <div class="metric-value" :class="fxReport.total_return >= 0 ? 'text-green' : 'text-red'">{{ (fxReport.total_return * 100).toFixed(2) }}%</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">年化收益</div>
            <div class="metric-value">{{ (fxReport.annual_return * 100).toFixed(2) }}%</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">夏普比率</div>
            <div class="metric-value">{{ fxReport.sharpe_ratio?.toFixed(3) ?? '-' }}</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">最大回撤</div>
            <div class="metric-value text-red">{{ (fxReport.max_drawdown * 100).toFixed(2) }}%</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">胜率</div>
            <div class="metric-value">{{ (fxReport.win_rate * 100).toFixed(1) }}%</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">盈亏比</div>
            <div class="metric-value">{{ fxReport.profit_factor?.toFixed(2) ?? '-' }}</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">成交笔数</div>
            <div class="metric-value">{{ fxReport.total_trades }}</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">最终权益</div>
            <div class="metric-value">{{ fxReport.final_equity?.toFixed(2) }}</div>
          </div>
        </div>

        <div class="chart-section">
          <h3>资金曲线（{{ fxSelectedSym }}）</h3>
          <div class="equity-chart">
            <svg viewBox="0 0 800 200" class="chart-svg">
              <polyline :points="fxEquityPoints" fill="none" stroke="var(--accent)" stroke-width="1.5" />
              <line x1="0" y1="100" x2="800" y2="100" stroke="var(--border)" stroke-width="0.5" stroke-dasharray="4" />
            </svg>
          </div>
        </div>

        <div class="trades-section">
          <h3>交易明细（最近 20 笔）</h3>
          <table>
            <thead><tr><th>方向</th><th>品种</th><th>开仓价</th><th>平仓价</th><th>数量</th><th>盈亏</th><th>手续费</th><th>持仓Bars</th></tr></thead>
            <tbody>
              <tr v-for="(t, i) in fxTrades.slice(0, 20)" :key="i">
                <td :class="t.side === 'LONG' ? 'text-green' : 'text-red'">{{ t.side === 'LONG' ? '做多' : '做空' }}</td>
                <td>{{ t.symbol }}</td>
                <td>{{ t.entry_price?.toFixed(5) }}</td>
                <td>{{ t.exit_price?.toFixed(5) }}</td>
                <td>{{ t.quantity?.toFixed(2) }}</td>
                <td :class="t.pnl >= 0 ? 'text-green' : 'text-red'">{{ t.pnl?.toFixed(2) }}</td>
                <td>{{ t.fee?.toFixed(2) }}</td>
                <td>{{ t.bars_held }}</td>
              </tr>
              <tr v-if="!fxTrades.length"><td colspan="8" class="text-muted">无交易记录</td></tr>
            </tbody>
          </table>
        </div>
      </div>
      <div v-else-if="fxStatus === 'FAILED'" class="empty text-red">回测失败：{{ fxError }}</div>
      <div v-else-if="fxRunning || fxStatus === 'RUNNING' || fxStatus === 'PENDING'" class="empty">回测执行中（MT5 拉历史 + 回放），请稍候...</div>
      <div v-else class="empty">选择品种/周期/策略后提交 FX 同构回测</div>
    </div>

    <!-- ============ 币安 dual_ma（保留原有） ============ -->
    <div v-else>
      <div class="card run-bar">
        <div class="field">
          <label>策略</label>
          <select v-model="form.strategy_type" class="input">
            <option value="dual_ma">双均线</option>
          </select>
        </div>
        <div class="field">
          <label>交易对</label>
          <input v-model="form.symbol" class="input" placeholder="如 BTCUSDT" />
        </div>
        <div class="field">
          <label>周期</label>
          <select v-model="form.timeframe" class="input">
            <option v-for="tf in ['1m','5m','15m','1h','4h','1d']" :key="tf" :value="tf">{{ tf }}</option>
          </select>
        </div>
        <div class="field">
          <label>K线数</label>
          <input v-model.number="form.limit" class="input input-sm" type="number" />
        </div>
        <button class="btn btn-primary" @click="runBacktest" :disabled="running">{{ running ? '回测中...' : '提交回测' }}</button>
        <button class="btn btn-aligned" @click="fetchTasks">刷新任务</button>
      </div>

      <div class="toolbar">
        <select v-model="selectedTask" class="input" @change="loadResult">
          <option value="">选择回测任务</option>
          <option v-for="t in tasks" :key="t.task_id" :value="t.task_id">
            {{ t.strategy_type }} {{ t.symbol }} {{ t.timeframe }} - {{ formatTime(t.created_at) }} [{{ t.status }}]
          </option>
        </select>
      </div>

      <div v-if="report" class="report">
        <div class="metrics-grid">
          <div class="metric-card">
            <div class="metric-label">总收益率</div>
            <div class="metric-value" :class="report.total_return >= 0 ? 'text-green' : 'text-red'">{{ (report.total_return * 100).toFixed(2) }}%</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">年化收益</div>
            <div class="metric-value">{{ (report.annual_return * 100).toFixed(2) }}%</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">夏普比率</div>
            <div class="metric-value">{{ report.sharpe_ratio?.toFixed(3) }}</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">最大回撤</div>
            <div class="metric-value text-red">{{ (report.max_drawdown * 100).toFixed(2) }}%</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">胜率</div>
            <div class="metric-value">{{ (report.win_rate * 100).toFixed(1) }}%</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">盈亏比</div>
            <div class="metric-value">{{ report.profit_factor?.toFixed(2) }}</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">总交易次数</div>
            <div class="metric-value">{{ report.total_trades }}</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">最终权益</div>
            <div class="metric-value">{{ report.final_equity?.toFixed(0) }}</div>
          </div>
        </div>

        <div class="chart-section">
          <h3>资金曲线</h3>
          <div class="equity-chart">
            <svg viewBox="0 0 800 200" class="chart-svg">
              <polyline :points="equityPoints" fill="none" stroke="var(--accent)" stroke-width="1.5" />
              <line x1="0" y1="100" x2="800" y2="100" stroke="var(--border)" stroke-width="0.5" stroke-dasharray="4" />
            </svg>
          </div>
        </div>

        <div class="trades-section">
          <h3>交易明细 (最近 20 笔)</h3>
          <table>
            <thead><tr><th>方向</th><th>品种</th><th>开仓价</th><th>平仓价</th><th>数量</th><th>盈亏</th><th>手续费</th><th>持仓Bars</th></tr></thead>
            <tbody>
              <tr v-for="(t, i) in trades.slice(0, 20)" :key="i">
                <td :class="t.side === 'LONG' ? 'text-green' : 'text-red'">{{ t.side === 'LONG' ? '做多' : '做空' }}</td>
                <td>{{ t.symbol }}</td>
                <td>{{ t.entry_price?.toFixed(2) }}</td>
                <td>{{ t.exit_price?.toFixed(2) }}</td>
                <td>{{ t.quantity?.toFixed(4) }}</td>
                <td :class="t.pnl >= 0 ? 'text-green' : 'text-red'">{{ t.pnl?.toFixed(2) }}</td>
                <td>{{ t.fee?.toFixed(2) }}</td>
                <td>{{ t.bars_held }}</td>
              </tr>
              <tr v-if="!trades.length"><td colspan="8" class="text-muted">无交易记录</td></tr>
            </tbody>
          </table>
        </div>
      </div>
      <div v-else-if="taskStatus === 'RUNNING' || taskStatus === 'PENDING'" class="empty">回测执行中，请稍候刷新...</div>
      <div v-else-if="taskStatus === 'FAILED'" class="empty text-red">回测失败</div>
      <div v-else class="empty">选择回测任务查看报告，或提交新回测</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'

const API = 'http://localhost:8000'

const engine = ref<'fx' | 'binance'>('fx')

/* ---------------- FX 同构回测 ---------------- */
interface FxReport {
  total_return: number; annual_return: number; sharpe_ratio: number | null
  max_drawdown: number; win_rate: number; profit_factor: number | null
  total_trades: number; final_equity: number; initial_capital: number
  total_fees: number; avg_win: number; avg_loss: number
  sortino_ratio: number | null; calmar_ratio: number | null
}
interface FxTrade { symbol: string; side: string; entry_price: number; exit_price: number; quantity: number; entry_time: number; exit_time: number; pnl: number; fee: number; bars_held: number }
interface FxSymResult { report: FxReport; equity_curve: number[]; trades: FxTrade[]; n_bars: number }

const fxForm = ref({ symbols: 'EURUSD', period: '1m', strategy: 'fx_simple_test', bars: 1500, capital: 10000 })
const fxStrategies = ref<string[]>([])
const fxResults = ref<Record<string, FxSymResult>>({})
const fxSymbols = ref<string[]>([])
const fxSelectedSym = ref('')
const fxStatus = ref('')
const fxError = ref('')
const fxRunning = ref(false)
let fxTaskId = ''
let fxPollTimer: ReturnType<typeof setTimeout> | null = null

const fxReport = computed<FxReport | null>(() => fxResults.value[fxSelectedSym.value]?.report ?? null)
const fxTrades = computed<FxTrade[]>(() => fxResults.value[fxSelectedSym.value]?.trades ?? [])
const fxEquityPoints = computed(() => {
  const curve = fxResults.value[fxSelectedSym.value]?.equity_curve ?? []
  if (!curve.length) return ''
  const max = Math.max(...curve), min = Math.min(...curve)
  const range = max - min || 1
  return curve.map((v, i) => `${(i / (curve.length - 1)) * 800},${200 - ((v - min) / range) * 180 - 10}`).join(' ')
})

async function fetchFxStrategies() {
  try {
    const resp = await fetch(`${API}/api/backtest/fx/strategies`)
    if (resp.ok) {
      const data = await resp.json()
      fxStrategies.value = data.strategies || []
      if (fxStrategies.value.length && !fxStrategies.value.includes(fxForm.value.strategy)) {
        fxForm.value.strategy = fxStrategies.value[0]
      }
    }
  } catch (e) { /* ignore */ }
}

async function runFx() {
  fxRunning.value = true
  fxStatus.value = 'PENDING'
  fxError.value = ''
  fxResults.value = {}
  fxSymbols.value = []
  const symbols = fxForm.value.symbols.split(',').map(s => s.trim()).filter(Boolean)
  try {
    const resp = await fetch(`${API}/api/backtest/fx/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        symbols,
        period: fxForm.value.period,
        strategy: fxForm.value.strategy,
        history_bars: fxForm.value.bars,
        capital: fxForm.value.capital,
      }),
    })
    if (!resp.ok) { fxRunning.value = false; fxStatus.value = 'FAILED'; fxError.value = `HTTP ${resp.status}`; return }
    const data = await resp.json()
    fxTaskId = data.task_id
    pollFxResult(0)
  } catch (e) { fxRunning.value = false; fxStatus.value = 'FAILED'; fxError.value = String(e) }
}

function pollFxResult(attempt: number) {
  if (fxPollTimer) clearTimeout(fxPollTimer)
  if (attempt > 60) { fxRunning.value = false; fxStatus.value = 'FAILED'; fxError.value = '超时'; return }
  fxPollTimer = setTimeout(async () => {
    try {
      const resp = await fetch(`${API}/api/backtest/fx/result/${fxTaskId}`)
      if (!resp.ok) { pollFxResult(attempt + 1); return }
      const data = await resp.json()
      fxStatus.value = data.status
      if (data.status === 'COMPLETED') {
        fxResults.value = data.results || {}
        fxSymbols.value = Object.keys(fxResults.value)
        fxSelectedSym.value = fxSymbols.value[0] || ''
        fxRunning.value = false
      } else if (data.status === 'FAILED') {
        fxError.value = data.error || '未知错误'
        fxRunning.value = false
      } else {
        pollFxResult(attempt + 1)
      }
    } catch (e) { pollFxResult(attempt + 1) }
  }, 2000)
}

/* ---------------- 币安 dual_ma（保留原有） ---------------- */
interface BacktestTask { task_id: string; strategy_type: string; symbol: string; timeframe: string; status: string; created_at: number }
interface Report {
  total_return: number; annual_return: number; sharpe_ratio: number
  max_drawdown: number; win_rate: number; profit_factor: number
  total_trades: number; final_equity: number; initial_capital: number
  total_fees: number; avg_win: number; avg_loss: number
}

const tasks = ref<BacktestTask[]>([])
const selectedTask = ref('')
const report = ref<Report | null>(null)
const equityCurve = ref<number[]>([])
const trades = ref<any[]>([])
const taskStatus = ref('')
const running = ref(false)
const form = ref({ strategy_type: 'dual_ma', symbol: 'BTCUSDT', timeframe: '1h', limit: 500 })

const equityPoints = computed(() => {
  const curve = equityCurve.value
  if (!curve.length) return ''
  const max = Math.max(...curve), min = Math.min(...curve)
  const range = max - min || 1
  return curve.map((v, i) => `${(i / (curve.length - 1)) * 800},${200 - ((v - min) / range) * 180 - 10}`).join(' ')
})

function formatTime(ts: number) { return ts ? new Date(ts).toLocaleString('zh-CN') : '-' }

async function fetchTasks() {
  try {
    const resp = await fetch(`${API}/api/backtest/tasks`)
    if (resp.ok) { const data = await resp.json(); tasks.value = data.tasks || [] }
  } catch (e) { /* ignore */ }
}

async function runBacktest() {
  running.value = true
  try {
    const resp = await fetch(`${API}/api/backtest/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_type: form.value.strategy_type, symbol: form.value.symbol,
        timeframe: form.value.timeframe, limit: form.value.limit,
        initial_capital: 100000, parameters: { fast_period: 7, slow_period: 25 },
      }),
    })
    if (resp.ok) {
      const data = await resp.json()
      selectedTask.value = data.task_id
      setTimeout(async () => { await fetchTasks(); await loadResult(); running.value = false }, 3000)
    } else { running.value = false }
  } catch (e) { running.value = false }
}

async function loadResult() {
  if (!selectedTask.value) { report.value = null; taskStatus.value = ''; return }
  try {
    const resp = await fetch(`${API}/api/backtest/result/${selectedTask.value}`)
    if (!resp.ok) return
    const data = await resp.json()
    taskStatus.value = data.status
    if (data.status === 'COMPLETED') {
      report.value = data.report
      equityCurve.value = data.equity_curve || []
      const tResp = await fetch(`${API}/api/backtest/tasks/${selectedTask.value}/trades?limit=100`)
      if (tResp.ok) { const tData = await tResp.json(); trades.value = tData.trades || [] }
    } else { report.value = null; trades.value = [] }
  } catch (e) { /* ignore */ }
}

onMounted(() => { fetchFxStrategies(); fetchTasks() })
</script>

<style scoped>
h2 { margin-bottom: 16px; } h3 { margin-bottom: 12px; font-size: 14px; }
.engine-tabs { display: flex; gap: 8px; margin-bottom: 16px; }
.tab { background: var(--bg-secondary); border: 1px solid var(--border); color: var(--text-secondary); padding: 6px 16px; border-radius: 6px; cursor: pointer; }
.tab.active { color: var(--text-primary); border-color: var(--accent); background: var(--bg-primary); }
.run-bar { display: flex; gap: 12px; align-items: flex-end; margin-bottom: 16px; flex-wrap: wrap; }
.field { display: flex; flex-direction: column; gap: 4px; }
.field label { font-size: 12px; color: var(--text-secondary); }
.btn-aligned { align-self: flex-end; }
.toolbar { display: flex; gap: 12px; margin-bottom: 20px; align-items: center; }
.input { background: var(--bg-primary); border: 1px solid var(--border); color: var(--text-primary); padding: 6px 12px; border-radius: 6px; }
.input-sm { width: 90px; }
.metrics-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px; }
.metric-card { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 14px; text-align: center; }
.metric-label { font-size: 12px; color: var(--text-secondary); margin-bottom: 6px; }
.metric-value { font-size: 18px; font-weight: 700; }
.chart-section { margin-bottom: 24px; background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
.chart-svg { width: 100%; height: auto; }
.trades-section { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
table { width: 100%; font-size: 12px; border-collapse: collapse; }
th, td { padding: 8px; text-align: left; border-bottom: 1px solid var(--border); }
th { color: var(--text-secondary); font-weight: 500; }
.empty { text-align: center; color: var(--text-secondary); padding: 60px; }
.text-green { color: var(--success); } .text-red { color: var(--danger); }
.text-muted { color: var(--text-secondary); }
</style>
