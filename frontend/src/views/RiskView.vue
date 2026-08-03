<template>
  <div>
    <h2>风控面板</h2>
    <div class="grid">
      <div class="card">
        <h3>风控规则</h3>
        <table>
          <thead><tr><th>规则</th><th>状态</th><th>参数</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="rule in rules" :key="rule.name">
              <td>{{ rule.label }}</td>
              <td><span class="badge" :class="rule.enabled ? 'badge-on' : 'badge-off'">{{ rule.enabled ? '启用' : '禁用' }}</span></td>
              <td class="params">{{ formatParams(rule.params) }}</td>
              <td><button class="btn btn-sm" @click="toggleRule(rule)">{{ rule.enabled ? '禁用' : '启用' }}</button></td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="card">
        <h3>风控触发记录</h3>
        <div class="log-list">
          <div v-for="(log, i) in riskLogs" :key="i" class="log-item" :class="'level-' + log.level?.toLowerCase()">
            <span class="log-time">{{ formatTime(log.timestamp) }}</span>
            <span class="log-rule">{{ log.rule_name }}</span>
            <span class="log-reason">{{ log.reason }}</span>
            <span class="log-level">{{ log.level }}</span>
          </div>
          <div v-if="!riskLogs.length" class="empty">暂无触发记录</div>
        </div>
      </div>

      <div class="card">
        <h3>风控概览</h3>
        <div class="stats">
          <div class="stat"><span class="stat-label">今日检查</span><span class="stat-value">{{ stats.total_checks }}</span></div>
          <div class="stat"><span class="stat-label">今日拒绝</span><span class="stat-value text-red">{{ stats.total_rejects }}</span></div>
          <div class="stat"><span class="stat-label">拒绝率</span><span class="stat-value">{{ stats.reject_rate }}%</span></div>
          <div class="stat"><span class="stat-label">引擎状态</span><span class="stat-value" :class="stats.running ? 'text-green' : 'text-red'">{{ stats.running ? '运行中' : '已停止' }}</span></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

interface RiskRule { name: string; label: string; enabled: boolean; params: Record<string, any> }
interface RiskLog { timestamp: number; rule_name: string; reason: string; level: string }

const rules = ref<RiskRule[]>([
  { name: 'max_order_amount', label: '单笔最大金额', enabled: true, params: { max_amount: 10000 } },
  { name: 'max_position_per_symbol', label: '单品种最大持仓', enabled: true, params: { max_quantity: 100 } },
  { name: 'max_total_position', label: '总持仓上限', enabled: true, params: { max_total_notional: 100000 } },
  { name: 'max_daily_loss', label: '单日最大亏损', enabled: true, params: { max_loss: 5000 } },
  { name: 'max_strategy_loss', label: '单策略最大亏损', enabled: true, params: { max_loss: 2000 } },
  { name: 'order_frequency', label: '下单频率限制', enabled: true, params: { max_orders: 60, window_seconds: 60 } },
  { name: 'price_deviation', label: '价格偏离保护', enabled: true, params: { max_deviation: 0.05 } },
  { name: 'min_order_quantity', label: '最小下单量', enabled: true, params: { min_qty: 0.001 } },
  { name: 'available_balance', label: '可用资金检查', enabled: true, params: {} },
  { name: 'consecutive_loss', label: '连续亏损限制', enabled: true, params: { max_consecutive: 5 } },
  { name: 'night_trading', label: '夜间交易限制', enabled: false, params: { start_hour: 0, end_hour: 6 } },
  { name: 'new_symbol', label: '新品种限制', enabled: false, params: {} },
])

const riskLogs = ref<RiskLog[]>([])
const stats = ref({ total_checks: 0, total_rejects: 0, reject_rate: '0.0', running: true })

function formatParams(params: Record<string, any>) {
  return Object.entries(params).map(([k, v]) => `${k}=${v}`).join(', ') || '-'
}

function formatTime(ts: number) { return ts ? new Date(ts).toLocaleTimeString('zh-CN') : '-' }

async function toggleRule(rule: RiskRule) {
  rule.enabled = !rule.enabled
  try {
    await fetch(`http://localhost:8000/api/v1/risk/rules/${rule.name}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: rule.enabled }),
    })
  } catch (e) { /* ignore */ }
}

async function fetchLogs() {
  try {
    const resp = await fetch('http://localhost:8000/api/v1/risk/logs?limit=50')
    if (resp.ok) riskLogs.value = await resp.json()
  } catch (e) { /* ignore */ }
}

async function fetchStats() {
  try {
    const resp = await fetch('http://localhost:8000/api/v1/risk/stats')
    if (resp.ok) stats.value = await resp.json()
  } catch (e) { /* ignore */ }
}

onMounted(() => { fetchLogs(); fetchStats() })
</script>

<style scoped>
h2 { margin-bottom: 16px; } h3 { margin-bottom: 12px; font-size: 14px; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.card { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
.card:last-child { grid-column: span 2; }
table { width: 100%; font-size: 12px; border-collapse: collapse; }
th, td { padding: 8px; text-align: left; border-bottom: 1px solid var(--border); }
th { color: var(--text-secondary); }
.params { font-size: 11px; color: var(--text-secondary); max-width: 200px; overflow: hidden; text-overflow: ellipsis; }
.badge { padding: 2px 8px; border-radius: 4px; font-size: 11px; }
.badge-on { background: rgba(0,212,170,0.15); color: var(--success); }
.badge-off { background: rgba(255,255,255,0.05); color: var(--text-secondary); }
.btn-sm { padding: 3px 8px; font-size: 11px; }
.log-list { max-height: 300px; overflow-y: auto; }
.log-item { display: flex; gap: 12px; padding: 6px 0; border-bottom: 1px solid var(--border); font-size: 12px; align-items: center; }
.log-time { color: var(--text-secondary); min-width: 70px; }
.log-rule { font-weight: 600; min-width: 120px; }
.log-reason { flex: 1; color: var(--text-secondary); }
.log-level { font-size: 11px; padding: 1px 6px; border-radius: 3px; }
.level-critical { border-left: 2px solid var(--danger); }
.level-warning { border-left: 2px solid #f0b90b; }
.stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; }
.stat { text-align: center; }
.stat-label { display: block; font-size: 12px; color: var(--text-secondary); margin-bottom: 4px; }
.stat-value { font-size: 20px; font-weight: 700; }
.empty { text-align: center; color: var(--text-secondary); padding: 20px; }
</style>
