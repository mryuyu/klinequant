<template>
  <div>
    <h2>告警中心</h2>
    <div class="tabs">
      <button :class="{active: tab==='events'}" @click="tab='events'">告警事件</button>
      <button :class="{active: tab==='rules'}" @click="tab='rules'">告警规则</button>
      <button :class="{active: tab==='channels'}" @click="tab='channels'">通知渠道</button>
    </div>

    <!-- 告警事件 -->
    <div v-if="tab==='events'" class="section">
      <div class="toolbar">
        <select v-model="eventFilter.level" class="input" @change="fetchEvents">
          <option value="">全部级别</option>
          <option value="INFO">INFO</option>
          <option value="WARNING">WARNING</option>
          <option value="CRITICAL">CRITICAL</option>
          <option value="FATAL">FATAL</option>
        </select>
        <button class="btn" @click="fetchEvents">刷新</button>
        <button class="btn btn-warning" @click="sendTest">发送测试告警</button>
      </div>
      <div class="event-list">
        <div v-for="ev in events" :key="ev.alert_id" class="event-card" :class="'lv-' + ev.level?.toLowerCase()">
          <div class="ev-header">
            <span class="ev-level">{{ ev.level }}</span>
            <span class="ev-title">{{ ev.title || ev.rule_name }}</span>
            <span class="ev-time">{{ formatTime(ev.timestamp) }}</span>
          </div>
          <div class="ev-body">
            <span class="ev-msg">{{ ev.message }}</span>
            <span class="ev-source">来源: {{ ev.source }}</span>
          </div>
          <div class="ev-footer">
            <span v-if="ev.acknowledged" class="ev-ack">✓ 已确认</span>
            <button v-else class="btn btn-sm" @click="ackEvent(ev.alert_id)">确认</button>
          </div>
        </div>
        <div v-if="!events.length" class="empty">暂无告警事件</div>
      </div>
    </div>

    <!-- 告警规则 -->
    <div v-if="tab==='rules'" class="section">
      <div class="toolbar">
        <button class="btn" @click="fetchRules">刷新</button>
      </div>
      <table>
        <thead><tr><th>规则名</th><th>级别</th><th>描述</th><th>冷却(s)</th><th>状态</th><th>操作</th></tr></thead>
        <tbody>
          <tr v-for="r in rules" :key="r.name">
            <td class="mono">{{ r.name }}</td>
            <td><span class="level-badge" :class="'lv-' + r.level?.toLowerCase()">{{ r.level }}</span></td>
            <td>{{ r.description }}</td>
            <td>{{ r.cooldown_seconds }}</td>
            <td><span :class="r.enabled ? 'text-green' : 'text-muted'">{{ r.enabled ? '启用' : '禁用' }}</span></td>
            <td><button class="btn btn-sm" @click="toggleRule(r)">{{ r.enabled ? '禁用' : '启用' }}</button></td>
          </tr>
        </tbody>
      </table>
      <div v-if="!rules.length" class="empty">暂无规则</div>
    </div>

    <!-- 通知渠道 -->
    <div v-if="tab==='channels'" class="section">
      <div class="toolbar">
        <button class="btn" @click="fetchChannels">刷新</button>
      </div>
      <div class="channel-grid">
        <div v-for="ch in channels" :key="ch.name" class="channel-card">
          <div class="ch-icon">{{ channelIcon(ch.type) }}</div>
          <div class="ch-info">
            <div class="ch-name">{{ ch.name }}</div>
            <div class="ch-type">{{ ch.type }}</div>
          </div>
          <span :class="ch.enabled ? 'text-green' : 'text-muted'">{{ ch.enabled ? '启用' : '禁用' }}</span>
          <button class="btn btn-sm btn-danger" @click="removeChannel(ch.name)">删除</button>
        </div>
        <div v-if="!channels.length" class="empty">暂未配置通知渠道</div>
      </div>

      <div class="card add-channel">
        <h3>添加渠道</h3>
        <div class="form-row">
          <select v-model="newChannel.type" class="input">
            <option value="dingtalk">钉钉</option>
            <option value="feishu">飞书</option>
            <option value="telegram">Telegram</option>
            <option value="webhook">通用 Webhook</option>
          </select>
          <input v-model="newChannel.name" placeholder="名称" class="input" />
        </div>
        <input v-model="newChannel.webhook_url" placeholder="Webhook URL / Bot Token" class="input full" />
        <input v-model="newChannel.secret" placeholder="Secret / Chat ID (可选)" class="input full" />
        <button class="btn btn-primary" @click="addChannel">添加</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

const API = 'http://localhost:8000'
const tab = ref('events')

// 事件
interface AlertEvent { alert_id: string; rule_name: string; level: string; title: string; message: string; source: string; timestamp: number; acknowledged: boolean }
const events = ref<AlertEvent[]>([])
const eventFilter = ref({ level: '' })

// 规则
interface AlertRule { name: string; level: string; description: string; enabled: boolean; cooldown_seconds: number }
const rules = ref<AlertRule[]>([])

// 渠道
interface Channel { name: string; type: string; enabled: boolean }
const channels = ref<Channel[]>([])
const newChannel = ref({ type: 'dingtalk', name: '', webhook_url: '', secret: '' })

function formatTime(ts: number) { return ts ? new Date(ts).toLocaleString('zh-CN') : '-' }
function channelIcon(type: string) {
  const map: Record<string, string> = { dingtalk: '🔔', feishu: '🐦', telegram: '✈️', webhook: '🔗' }
  return map[type] || '📢'
}

async function fetchEvents() {
  try {
    const params = new URLSearchParams({ limit: '100' })
    if (eventFilter.value.level) params.set('level', eventFilter.value.level)
    const resp = await fetch(`${API}/api/alerts/events?${params}`)
    if (resp.ok) events.value = await resp.json()
  } catch (e) { /* ignore */ }
}

async function ackEvent(id: string) {
  try {
    await fetch(`${API}/api/alerts/events/${id}/ack`, { method: 'POST' })
    const ev = events.value.find(e => e.alert_id === id)
    if (ev) ev.acknowledged = true
  } catch (e) { /* ignore */ }
}

async function sendTest() {
  try {
    await fetch(`${API}/api/alerts/test`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: '手动测试告警 - ' + new Date().toLocaleTimeString(), level: 'WARNING' }),
    })
    await fetchEvents()
  } catch (e) { /* ignore */ }
}

async function fetchRules() {
  try {
    const resp = await fetch(`${API}/api/alerts/rules`)
    if (resp.ok) rules.value = await resp.json()
  } catch (e) { /* ignore */ }
}

async function toggleRule(r: AlertRule) {
  try {
    await fetch(`${API}/api/alerts/rules/${r.name}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: !r.enabled }),
    })
    r.enabled = !r.enabled
  } catch (e) { /* ignore */ }
}

async function fetchChannels() {
  try {
    const resp = await fetch(`${API}/api/alerts/channels`)
    if (resp.ok) channels.value = await resp.json()
  } catch (e) { /* ignore */ }
}

async function addChannel() {
  if (!newChannel.value.name || !newChannel.value.webhook_url) return
  try {
    await fetch(`${API}/api/alerts/channels`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(newChannel.value),
    })
    newChannel.value = { type: 'dingtalk', name: '', webhook_url: '', secret: '' }
    await fetchChannels()
  } catch (e) { /* ignore */ }
}

async function removeChannel(name: string) {
  try {
    await fetch(`${API}/api/alerts/channels/${name}`, { method: 'DELETE' })
    await fetchChannels()
  } catch (e) { /* ignore */ }
}

onMounted(() => { fetchEvents(); fetchRules(); fetchChannels() })
</script>

<style scoped>
h2 { margin-bottom: 16px; }
.tabs { display: flex; gap: 4px; margin-bottom: 20px; }
.tabs button { padding: 8px 16px; border: 1px solid var(--border); background: var(--bg-secondary); color: var(--text-secondary); border-radius: 6px; cursor: pointer; }
.tabs button.active { background: var(--accent); color: #fff; border-color: var(--accent); }
.toolbar { display: flex; gap: 12px; margin-bottom: 16px; align-items: center; }
.input { background: var(--bg-primary); border: 1px solid var(--border); color: var(--text-primary); padding: 6px 12px; border-radius: 6px; }
.full { width: 100%; margin-bottom: 8px; }

.event-list { display: flex; flex-direction: column; gap: 10px; }
.event-card { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; border-left: 3px solid var(--border); }
.lv-info { border-left-color: var(--accent); }
.lv-warning { border-left-color: #f0b90b; }
.lv-critical { border-left-color: var(--danger); }
.lv-fatal { border-left-color: #ff0000; }
.ev-header { display: flex; align-items: center; gap: 10px; margin-bottom: 6px; }
.ev-level { font-size: 11px; font-weight: 700; padding: 1px 6px; border-radius: 3px; }
.ev-title { font-weight: 600; font-size: 13px; }
.ev-time { margin-left: auto; font-size: 11px; color: var(--text-secondary); }
.ev-body { display: flex; justify-content: space-between; font-size: 12px; margin-bottom: 6px; }
.ev-msg { color: var(--text-primary); }
.ev-source { color: var(--text-secondary); }
.ev-footer { display: flex; justify-content: flex-end; }
.ev-ack { font-size: 12px; color: var(--success); }

table { width: 100%; font-size: 12px; border-collapse: collapse; }
th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border); }
th { color: var(--text-secondary); font-weight: 500; }
.mono { font-family: var(--font-mono); font-size: 11px; }
.level-badge { font-size: 10px; padding: 1px 6px; border-radius: 3px; font-weight: 600; }
.text-green { color: var(--success); }
.text-muted { color: var(--text-secondary); }

.channel-grid { display: flex; flex-direction: column; gap: 10px; margin-bottom: 20px; }
.channel-card { display: flex; align-items: center; gap: 12px; background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; }
.ch-icon { font-size: 20px; }
.ch-info { flex: 1; }
.ch-name { font-weight: 600; font-size: 13px; }
.ch-type { font-size: 11px; color: var(--text-secondary); }
.add-channel { margin-top: 16px; }
.add-channel h3 { margin-bottom: 12px; font-size: 14px; }
.form-row { display: flex; gap: 10px; margin-bottom: 8px; }
.btn-sm { padding: 3px 8px; font-size: 11px; }
.btn-warning { background: #f0b90b; color: #000; }
.btn-danger { background: var(--danger); color: #fff; }
.empty { text-align: center; color: var(--text-secondary); padding: 40px; }
</style>
