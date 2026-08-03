<template>
  <div>
    <h2>系统监控</h2>
    <div class="grid">
      <div class="card">
        <h3>引擎状态</h3>
        <div class="engine-list">
          <div v-for="eng in engines" :key="eng.name" class="engine-item">
            <span :class="eng.running ? 'dot-green' : 'dot-red'"></span>
            <span class="eng-name">{{ eng.label }}</span>
            <span class="eng-latency">{{ eng.latency_ms }}ms</span>
            <span class="eng-uptime">{{ eng.uptime }}</span>
          </div>
        </div>
      </div>

      <div class="card">
        <h3>资源占用</h3>
        <div class="resource-list">
          <div class="resource">
            <span class="res-label">CPU</span>
            <div class="progress-bar"><div class="progress-fill" :style="{width: resources.cpu + '%'}"></div></div>
            <span class="res-value">{{ resources.cpu }}%</span>
          </div>
          <div class="resource">
            <span class="res-label">内存</span>
            <div class="progress-bar"><div class="progress-fill" :style="{width: resources.memory + '%'}"></div></div>
            <span class="res-value">{{ resources.memory }}%</span>
          </div>
          <div class="resource">
            <span class="res-label">磁盘</span>
            <div class="progress-bar"><div class="progress-fill" :style="{width: resources.disk + '%'}"></div></div>
            <span class="res-value">{{ resources.disk }}%</span>
          </div>
        </div>
      </div>

      <div class="card">
        <h3>WebSocket 连接</h3>
        <div class="ws-info">
          <div class="stat"><span class="stat-label">活跃连接</span><span class="stat-value">{{ wsStats.active }}</span></div>
          <div class="stat"><span class="stat-label">消息/秒</span><span class="stat-value">{{ wsStats.msg_per_sec }}</span></div>
          <div class="stat"><span class="stat-label">订阅主题</span><span class="stat-value">{{ wsStats.subscriptions }}</span></div>
          <div class="stat"><span class="stat-label">断线次数</span><span class="stat-value">{{ wsStats.reconnects }}</span></div>
        </div>
      </div>

      <div class="card">
        <h3>最近告警</h3>
        <div class="alert-list">
          <div v-for="(a, i) in alerts" :key="i" class="alert-item" :class="'alert-' + a.level?.toLowerCase()">
            <span class="alert-time">{{ formatTime(a.timestamp) }}</span>
            <span class="alert-msg">{{ a.message }}</span>
          </div>
          <div v-if="!alerts.length" class="empty">暂无告警</div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'

const API = 'http://localhost:8000'

interface Engine { name: string; label: string; running: boolean; latency_ms: number; uptime: string }

const engines = ref<Engine[]>([])
const resources = ref({ cpu: 0, memory: 0, disk: 0 })
const wsStats = ref({ active: 0, msg_per_sec: 0, subscriptions: 0, reconnects: 0 })
const alerts = ref<any[]>([])
const uptime = ref(0)
let timer: ReturnType<typeof setInterval> | null = null

function formatTime(ts: number) { return ts ? new Date(ts).toLocaleTimeString('zh-CN') : '-' }

async function fetchHealth() {
  try {
    const resp = await fetch(`${API}/api/system/health`)
    if (resp.ok) {
      const data = await resp.json()
      if (data.engines) engines.value = data.engines
      if (data.resources) resources.value = data.resources
      if (data.ws) wsStats.value = data.ws
      if (data.uptime_seconds) uptime.value = data.uptime_seconds
    }
  } catch (e) { /* ignore */ }
}

async function fetchAlerts() {
  try {
    const resp = await fetch(`${API}/api/system/alerts?limit=20`)
    if (resp.ok) alerts.value = await resp.json()
  } catch (e) { /* ignore */ }
}

onMounted(() => { fetchHealth(); fetchAlerts(); timer = setInterval(() => { fetchHealth(); fetchAlerts() }, 5000) })
onUnmounted(() => { if (timer) clearInterval(timer) })
</script>

<style scoped>
h2 { margin-bottom: 16px; } h3 { margin-bottom: 12px; font-size: 14px; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.card { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
.engine-list { display: flex; flex-direction: column; gap: 10px; }
.engine-item { display: flex; align-items: center; gap: 10px; font-size: 13px; }
.eng-name { flex: 1; }
.eng-latency { color: var(--text-secondary); font-size: 12px; }
.eng-uptime { color: var(--text-secondary); font-size: 12px; }
.dot-green, .dot-red { width: 8px; height: 8px; border-radius: 50%; }
.dot-green { background: var(--success); }
.dot-red { background: var(--danger); }
.resource-list { display: flex; flex-direction: column; gap: 14px; }
.resource { display: flex; align-items: center; gap: 10px; }
.res-label { width: 40px; font-size: 12px; color: var(--text-secondary); }
.progress-bar { flex: 1; height: 6px; background: var(--bg-primary); border-radius: 3px; overflow: hidden; }
.progress-fill { height: 100%; background: var(--accent); border-radius: 3px; transition: width 0.5s; }
.res-value { width: 40px; font-size: 12px; text-align: right; }
.ws-info { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }
.stat { text-align: center; }
.stat-label { display: block; font-size: 11px; color: var(--text-secondary); margin-bottom: 4px; }
.stat-value { font-size: 18px; font-weight: 700; }
.alert-list { max-height: 200px; overflow-y: auto; }
.alert-item { display: flex; gap: 10px; padding: 6px 0; border-bottom: 1px solid var(--border); font-size: 12px; }
.alert-time { color: var(--text-secondary); min-width: 70px; }
.alert-critical { border-left: 2px solid var(--danger); padding-left: 8px; }
.alert-warning { border-left: 2px solid #f0b90b; padding-left: 8px; }
.alert-info { border-left: 2px solid var(--accent); padding-left: 8px; }
.empty { text-align: center; color: var(--text-secondary); padding: 20px; }
</style>
