<template>
  <div>
    <h2>信号监控</h2>
    <div class="toolbar">
      <select v-model="filter.direction" class="input">
        <option value="">全部方向</option>
        <option value="LONG">做多</option>
        <option value="SHORT">做空</option>
        <option value="CLOSE">平仓</option>
      </select>
      <select v-model="filter.status" class="input">
        <option value="">全部状态</option>
        <option value="PENDING">待处理</option>
        <option value="EXECUTED">已执行</option>
        <option value="EXPIRED">已过期</option>
        <option value="REJECTED">已拒绝</option>
      </select>
      <span class="badge">{{ filteredSignals.length }} 条信号</span>
    </div>

    <div class="signal-list">
      <div v-for="sig in filteredSignals" :key="sig.signal_id" class="signal-card" :class="'sig-' + sig.direction?.toLowerCase()">
        <div class="sig-header">
          <span class="sig-direction" :class="'dir-' + sig.direction?.toLowerCase()">
            {{ directionLabel(sig.direction) }}
          </span>
          <span class="sig-symbol">{{ sig.symbol }}</span>
          <span class="sig-strength">{{ '★'.repeat(sig.strength || 1) }}</span>
          <span class="sig-time">{{ formatTime(sig.timestamp) }}</span>
        </div>
        <div class="sig-body">
          <div class="sig-reason">{{ sig.reason }}</div>
          <div class="sig-meta">
            <span>价格: {{ sig.price }}</span>
            <span>策略: {{ sig.strategy_id }}</span>
            <span v-if="sig.suggested_quantity">建议量: {{ sig.suggested_quantity }}</span>
          </div>
        </div>
        <div class="sig-footer">
          <span class="sig-status" :class="'st-' + sig.status?.toLowerCase()">{{ statusLabel(sig.status) }}</span>
          <div v-if="sig.status === 'PENDING'" class="sig-actions">
            <button class="btn btn-sm btn-success" @click="confirmSignal(sig)">确认下单</button>
            <button class="btn btn-sm btn-danger" @click="rejectSignal(sig)">忽略</button>
          </div>
        </div>
      </div>
      <div v-if="!filteredSignals.length" class="empty">暂无信号</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'

interface Signal {
  signal_id: string
  strategy_id: string
  symbol: string
  direction: string
  strength: number
  price: number
  reason: string
  timestamp: number
  suggested_quantity?: number
  status: string
}

const signals = ref<Signal[]>([])
const filter = ref({ direction: '', status: '' })
let ws: WebSocket | null = null
let pollTimer: ReturnType<typeof setInterval> | null = null

const filteredSignals = computed(() => {
  return signals.value.filter(s => {
    if (filter.value.direction && s.direction !== filter.value.direction) return false
    if (filter.value.status && s.status !== filter.value.status) return false
    return true
  })
})

function directionLabel(dir: string) {
  const map: Record<string, string> = { LONG: '做多', SHORT: '做空', CLOSE: '平仓', NEUTRAL: '中性' }
  return map[dir] || dir
}

function statusLabel(status: string) {
  const map: Record<string, string> = { PENDING: '待处理', CONFIRMED: '已确认', EXECUTED: '已执行', EXPIRED: '已过期', REJECTED: '已拒绝' }
  return map[status] || status
}

function formatTime(ts: number) {
  return new Date(ts).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

async function confirmSignal(sig: Signal) {
  try {
    await fetch('http://localhost:8000/api/v1/trade/signals/' + sig.signal_id + '/confirm', { method: 'POST' })
    sig.status = 'CONFIRMED'
  } catch (e) { console.error(e) }
}

async function rejectSignal(sig: Signal) {
  sig.status = 'REJECTED'
}

async function fetchSignals() {
  try {
    const resp = await fetch('http://localhost:8000/api/v1/signals?limit=50')
    if (resp.ok) signals.value = await resp.json()
  } catch (e) { /* ignore */ }
}

function connectWS() {
  ws = new WebSocket('ws://localhost:8000/ws')
  ws.onopen = () => ws?.send(JSON.stringify({ action: 'subscribe', topic: 'signals' }))
  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data)
    if (msg.topic === 'signals' && msg.data) {
      signals.value.unshift(msg.data)
      if (signals.value.length > 200) signals.value.pop()
    }
  }
  ws.onclose = () => setTimeout(connectWS, 3000)
}

onMounted(() => { fetchSignals(); connectWS() })
onUnmounted(() => { ws?.close(); if (pollTimer) clearInterval(pollTimer) })
</script>

<style scoped>
h2 { margin-bottom: 16px; }
.toolbar { display: flex; gap: 12px; align-items: center; margin-bottom: 16px; }
.input { background: var(--bg-primary); border: 1px solid var(--border); color: var(--text-primary); padding: 6px 12px; border-radius: 6px; }
.badge { font-size: 12px; color: var(--text-secondary); }
.signal-list { display: flex; flex-direction: column; gap: 12px; }
.signal-card { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 14px; border-left: 3px solid var(--border); }
.sig-long { border-left-color: var(--success); }
.sig-short { border-left-color: var(--danger); }
.sig-close { border-left-color: var(--warning, #f0b90b); }
.sig-header { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; }
.sig-direction { font-weight: 700; font-size: 13px; }
.dir-long { color: var(--success); }
.dir-short { color: var(--danger); }
.dir-close { color: var(--warning, #f0b90b); }
.sig-symbol { font-weight: 600; }
.sig-strength { color: var(--warning, #f0b90b); font-size: 12px; }
.sig-time { margin-left: auto; font-size: 12px; color: var(--text-secondary); }
.sig-body { margin-bottom: 8px; }
.sig-reason { font-size: 13px; color: var(--text-primary); margin-bottom: 4px; }
.sig-meta { display: flex; gap: 16px; font-size: 12px; color: var(--text-secondary); }
.sig-footer { display: flex; align-items: center; justify-content: space-between; }
.sig-status { font-size: 12px; padding: 2px 8px; border-radius: 4px; }
.st-pending { background: rgba(240,185,11,0.15); color: #f0b90b; }
.st-executed { background: rgba(0,212,170,0.15); color: var(--success); }
.st-expired { background: rgba(255,255,255,0.05); color: var(--text-secondary); }
.st-rejected { background: rgba(246,70,93,0.15); color: var(--danger); }
.sig-actions { display: flex; gap: 8px; }
.btn-sm { padding: 4px 10px; font-size: 12px; }
.empty { text-align: center; color: var(--text-secondary); padding: 40px; }
</style>
