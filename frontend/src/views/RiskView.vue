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

// 规则显示名映射：后端 /api/risk/rules 只下发 name/enabled/params，不含中文 label，
// 故 label 由前端按 name 映射，enabled/params 一律以后端为准（QH-003：废除硬编码规则表）
const RULE_LABELS: Record<string, string> = {
  max_order_amount: '单笔最大金额',
  max_position_per_symbol: '单品种最大持仓',
  max_total_position: '总持仓上限',
  max_daily_loss: '单日最大亏损',
  max_strategy_loss: '单策略最大亏损',
  order_frequency: '下单频率限制',
  price_deviation: '价格偏离保护',
  min_order_quantity: '最小下单量',
  available_balance: '可用资金检查',
  consecutive_loss: '连续亏损限制',
  night_trading: '夜间交易限制',
  new_symbol: '新品种限制',
}

const rules = ref<RiskRule[]>([])

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
    if (resp.ok) {
      const data = await resp.json()
      // 后端返回 { logs: [...], total }，此前直接把整个对象当数组赋值（QH-003）
      riskLogs.value = Array.isArray(data) ? data : (data.logs ?? [])
    }
  } catch (e) { /* ignore */ }
}

async function fetchRules() {
  try {
    const resp = await fetch('http://localhost:8000/api/v1/risk/rules')
    if (resp.ok) {
      const data = await resp.json()
      const list = Array.isArray(data) ? data : (data.rules ?? [])
      rules.value = list.map((r: any) => ({
        name: r.name,
        label: RULE_LABELS[r.name] ?? r.name,
        enabled: !!r.enabled,
        params: r.params ?? {},
      }))
    }
  } catch (e) { /* ignore */ }
}

async function fetchStats() {
  try {
    const resp = await fetch('http://localhost:8000/api/v1/risk/stats')
    if (resp.ok) {
      const s = await resp.json()
      // 后端字段为 total_rejected 且无 reject_rate，前端在此对齐并计算拒绝率（QH-003）
      const checks = Number(s.total_checks ?? 0)
      const rejects = Number(s.total_rejected ?? s.total_rejects ?? 0)
      stats.value = {
        total_checks: checks,
        total_rejects: rejects,
        reject_rate: checks > 0 ? ((rejects / checks) * 100).toFixed(1) : '0.0',
        running: !!s.running,
      }
    }
  } catch (e) { /* ignore */ }
}

onMounted(() => { fetchRules(); fetchLogs(); fetchStats() })
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
