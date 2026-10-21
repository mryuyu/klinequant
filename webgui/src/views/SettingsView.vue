<template>
  <div>
    <h2>系统设置</h2>

    <!-- 交易所配置 -->
    <div class="card section">
      <h3>交易所配置</h3>
      <div class="config-grid">
        <div class="config-item">
          <label>交易所</label>
          <span class="value">Binance (Demo)</span>
        </div>
        <div class="config-item">
          <label>REST 基址</label>
          <span class="value">{{ config.rest_base }}</span>
        </div>
        <div class="config-item">
          <label>API Key</label>
          <span class="value masked">{{ config.api_key_masked }}</span>
        </div>
        <div class="config-item">
          <label>连接状态</label>
          <span :class="config.connected ? 'text-green' : 'text-red'">{{ config.connected ? '已连接' : '未连接' }}</span>
        </div>
        <div class="config-item">
          <label>代理</label>
          <span class="value">{{ config.proxy || '直连' }}</span>
        </div>
      </div>
    </div>

    <!-- 通知渠道 -->
    <div class="card section">
      <h3>通知渠道</h3>
      <div class="channel-list">
        <div v-for="ch in channels" :key="ch.name" class="channel-item">
          <span class="ch-type">{{ ch.type }}</span>
          <span class="ch-name">{{ ch.name }}</span>
          <span :class="ch.enabled ? 'text-green' : 'text-muted'">{{ ch.enabled ? '启用' : '禁用' }}</span>
          <button class="btn btn-danger btn-sm" @click="removeChannel(ch.name)">删除</button>
        </div>
        <div v-if="!channels.length" class="text-muted">暂无通知渠道</div>
      </div>

      <div class="add-channel">
        <select v-model="newCh.type" class="input">
          <option value="dingtalk">钉钉</option>
          <option value="feishu">飞书</option>
          <option value="telegram">Telegram</option>
          <option value="webhook">Webhook</option>
        </select>
        <input v-model="newCh.webhook_url" class="input" placeholder="Webhook URL / Bot Token" />
        <input v-model="newCh.secret" class="input" placeholder="Secret / Chat ID (可选)" />
        <button class="btn btn-primary" @click="addChannel">添加渠道</button>
      </div>
    </div>

    <!-- 系统信息 -->
    <div class="card section">
      <h3>系统信息</h3>
      <div class="config-grid">
        <div class="config-item"><label>版本</label><span class="value">1.0.0</span></div>
        <div class="config-item"><label>运行时间</label><span class="value">{{ sysInfo.uptime }}</span></div>
        <div class="config-item"><label>WS 连接数</label><span class="value">{{ sysInfo.ws_active }}</span></div>
        <div class="config-item"><label>CPU</label><span class="value">{{ sysInfo.cpu }}%</span></div>
        <div class="config-item"><label>内存</label><span class="value">{{ sysInfo.memory }}%</span></div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'

const API = 'http://localhost:8000'

const config = reactive({
  rest_base: 'https://api.binance.com',
  api_key_masked: '****...****',
  connected: false,
  proxy: 'http://127.0.0.1:7897',
})

const channels = ref<any[]>([])
const newCh = reactive({ type: 'webhook', webhook_url: '', secret: '' })
const sysInfo = reactive({ uptime: '-', ws_active: 0, cpu: 0, memory: 0 })

async function fetchChannels() {
  try {
    const resp = await fetch(`${API}/api/alerts/channels`)
    const data = await resp.json()
    channels.value = data.channels || []
  } catch (e) { console.error(e) }
}

async function addChannel() {
  if (!newCh.webhook_url) return
  try {
    await fetch(`${API}/api/alerts/channels`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: newCh.type, webhook_url: newCh.webhook_url, secret: newCh.secret, enabled: true }),
    })
    newCh.webhook_url = ''
    newCh.secret = ''
    await fetchChannels()
  } catch (e) { console.error(e) }
}

async function removeChannel(name: string) {
  try {
    await fetch(`${API}/api/alerts/channels/${name}`, { method: 'DELETE' })
    await fetchChannels()
  } catch (e) { console.error(e) }
}

async function fetchHealth() {
  try {
    const resp = await fetch(`${API}/api/system/health`)
    const data = await resp.json()
    config.connected = data.status === 'healthy'
    const up = data.uptime_seconds || 0
    sysInfo.uptime = up > 3600 ? `${Math.floor(up / 3600)}h ${Math.floor((up % 3600) / 60)}m` : `${Math.floor(up / 60)}m`
    sysInfo.ws_active = data.ws?.active || 0
    sysInfo.cpu = data.resources?.cpu || 0
    sysInfo.memory = data.resources?.memory || 0
  } catch (e) { console.error(e) }
}

onMounted(() => {
  fetchChannels()
  fetchHealth()
})
</script>

<style scoped>
h2 { margin-bottom: 16px; }
h3 { margin-bottom: 12px; font-size: 14px; }
.section { margin-bottom: 20px; }
.config-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; }
.config-item { display: flex; flex-direction: column; gap: 4px; }
.config-item label { font-size: 12px; color: var(--text-secondary); }
.config-item .value { font-size: 14px; font-weight: 500; }
.masked { font-family: monospace; letter-spacing: 2px; }
.channel-list { margin-bottom: 16px; }
.channel-item { display: flex; align-items: center; gap: 12px; padding: 8px 0; border-bottom: 1px solid var(--border); }
.ch-type { background: rgba(59,130,246,.15); color: #3b82f6; padding: 2px 8px; border-radius: 4px; font-size: 12px; }
.ch-name { flex: 1; }
.add-channel { display: flex; gap: 8px; flex-wrap: wrap; }
.input { background: var(--bg-primary); border: 1px solid var(--border); color: var(--text-primary); padding: 6px 12px; border-radius: 6px; }
.btn-sm { padding: 2px 8px; font-size: 12px; }
.text-green { color: var(--success); }
.text-red { color: var(--danger); }
.text-muted { color: var(--text-secondary); }
</style>
