<template>
  <div>
    <h2>策略管理</h2>

    <!-- 创建策略 -->
    <div class="card create-bar">
      <select v-model="form.strategy_type">
        <option v-for="r in registered" :key="r" :value="r">{{ r }}</option>
      </select>
      <input v-model="form.name" placeholder="策略名称" />
      <input v-model="form.symbols" placeholder="交易对(逗号分隔)" />
      <button class="btn btn-primary" @click="createStrategy">创建策略</button>
    </div>

    <!-- 策略列表 -->
    <div class="card">
      <table>
        <thead><tr><th>名称</th><th>类型</th><th>品种</th><th>状态</th><th>操作</th></tr></thead>
        <tbody>
          <tr v-for="s in strategies" :key="s.strategy_id">
            <td>{{ s.name }}</td>
            <td>{{ s.strategy_type }}</td>
            <td>{{ (s.symbols || []).join(', ') }}</td>
            <td><span :class="'badge badge-' + s.status">{{ s.status }}</span></td>
            <td class="actions">
              <button v-if="s.status !== 'RUNNING'" class="btn btn-success btn-sm" @click="doAction(s.strategy_id, 'start')">启动</button>
              <button v-if="s.status === 'RUNNING'" class="btn btn-warning btn-sm" @click="doAction(s.strategy_id, 'pause')">暂停</button>
              <button v-if="s.status !== 'STOPPED'" class="btn btn-danger btn-sm" @click="doAction(s.strategy_id, 'stop')">停止</button>
              <button class="btn btn-sm" @click="showLogs(s.strategy_id)">日志</button>
              <button class="btn btn-danger btn-sm" @click="unload(s.strategy_id)">卸载</button>
            </td>
          </tr>
          <tr v-if="!strategies.length"><td colspan="5" class="text-muted">暂无策略，请先创建</td></tr>
        </tbody>
      </table>
    </div>

    <!-- 策略日志弹窗 -->
    <div v-if="logPanel" class="card log-panel">
      <h4>策略日志 <button class="btn btn-sm" @click="logPanel = null">关闭</button></h4>
      <div class="log-list">
        <div v-for="(l, i) in logs" :key="i" :class="'log-' + l.level">
          <span class="log-time">{{ new Date(l.timestamp).toLocaleTimeString() }}</span>
          {{ l.message }}
        </div>
        <div v-if="!logs.length" class="text-muted">无日志</div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

const API = 'http://localhost:8000'

interface Strategy {
  strategy_id: string
  name: string
  strategy_type: string
  status: string
  symbols: string[]
}

const strategies = ref<Strategy[]>([])
const registered = ref<string[]>([])
const logPanel = ref<string | null>(null)
const logs = ref<{ timestamp: number; level: string; message: string }[]>([])

const form = ref({
  strategy_type: 'dual_ma',
  name: '',
  symbols: 'BTCUSDT',
})

async function fetchStrategies() {
  try {
    const resp = await fetch(`${API}/api/strategies`)
    const data = await resp.json()
    strategies.value = data.strategies || []
  } catch (e) { console.error(e) }
}

async function fetchRegistered() {
  try {
    const resp = await fetch(`${API}/api/strategies/registered`)
    const data = await resp.json()
    registered.value = data.registered || []
    if (registered.value.length && !form.value.strategy_type) {
      form.value.strategy_type = registered.value[0]
    }
  } catch (e) { console.error(e) }
}

async function createStrategy() {
  const body = {
    strategy_type: form.value.strategy_type,
    name: form.value.name || form.value.strategy_type + '_' + Date.now(),
    symbols: form.value.symbols.split(',').map(s => s.trim()).filter(Boolean),
    timeframes: ['1h'],
    parameters: {},
  }
  try {
    await fetch(`${API}/api/strategies`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    form.value.name = ''
    await fetchStrategies()
  } catch (e) { console.error(e) }
}

async function doAction(id: string, action: 'start' | 'stop' | 'pause') {
  try {
    await fetch(`${API}/api/strategies/${id}/${action}`, { method: 'POST' })
    await fetchStrategies()
  } catch (e) { console.error(e) }
}

async function unload(id: string) {
  try {
    await fetch(`${API}/api/strategies/${id}`, { method: 'DELETE' })
    await fetchStrategies()
  } catch (e) { console.error(e) }
}

async function showLogs(id: string) {
  logPanel.value = id
  try {
    const resp = await fetch(`${API}/api/strategies/${id}/logs?limit=100`)
    const data = await resp.json()
    logs.value = data.logs || []
  } catch (e) { logs.value = [] }
}

onMounted(() => {
  fetchStrategies()
  fetchRegistered()
})
</script>

<style scoped>
h2 { margin-bottom: 16px; }
.create-bar { display: flex; gap: 8px; align-items: center; margin-bottom: 16px; flex-wrap: wrap; }
.create-bar select, .create-bar input { padding: 6px 10px; border-radius: 4px; border: 1px solid var(--border); background: var(--bg-secondary); color: var(--text); }
.actions { display: flex; gap: 4px; flex-wrap: wrap; }
.btn-sm { padding: 2px 8px; font-size: 12px; }
.btn-warning { background: #f59e0b; color: #000; }
.badge { padding: 2px 8px; border-radius: 4px; font-size: 12px; }
.badge-RUNNING { background: rgba(16,185,129,.15); color: var(--success); }
.badge-STOPPED { background: rgba(107,114,128,.15); color: var(--text-secondary); }
.badge-INITIALIZED { background: rgba(59,130,246,.15); color: #3b82f6; }
.badge-PAUSED { background: rgba(245,158,11,.15); color: #f59e0b; }
.log-panel { margin-top: 16px; }
.log-list { max-height: 300px; overflow-y: auto; font-family: monospace; font-size: 13px; }
.log-list > div { padding: 2px 0; }
.log-time { color: var(--text-secondary); margin-right: 8px; }
.log-ERROR { color: var(--danger); }
.log-WARN { color: #f59e0b; }
.log-INFO { color: var(--text); }
</style>
