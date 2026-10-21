import { defineStore } from 'pinia'
import { ref } from 'vue'

export interface KlineData {
  timestamp: number
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export interface TickerData {
  last_price: number
  bid: number
  ask: number
  volume_24h: number
  price_change_pct: number
  high_24h: number
  low_24h: number
}

const API_BASE = 'http://localhost:8000'
const WS_BASE = 'ws://localhost:8000'

export const useMarketStore = defineStore('market', () => {
  const symbol = ref('BTCUSDT')
  const timeframe = ref('1h')
  const klines = ref<KlineData[]>([])
  const lastPrice = ref(0)
  const ticker = ref<TickerData | null>(null)
  const connected = ref(false)
  const loading = ref(false)
  const wsConnected = ref(false)

  let ws: WebSocket | null = null
  let pollTimer: ReturnType<typeof setInterval> | null = null
  let tickerTimer: ReturnType<typeof setInterval> | null = null
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null

  /** 从 REST API 拉取历史 K 线 */
  async function fetchKlines() {
    loading.value = true
    try {
      const resp = await fetch(
        `${API_BASE}/api/market/klines?symbol=${symbol.value}&timeframe=${timeframe.value}&limit=200`
      )
      const json = await resp.json()
      if (json.data && json.data.length > 0) {
        klines.value = json.data
        lastPrice.value = json.data[json.data.length - 1].close
        connected.value = true
      }
    } catch (e) {
      console.error('Failed to fetch klines:', e)
      connected.value = false
    } finally {
      loading.value = false
    }
  }

  /** 获取 24h 行情摘要 */
  async function fetchTicker() {
    try {
      const resp = await fetch(`${API_BASE}/api/market/ticker?symbol=${symbol.value}`)
      const json = await resp.json()
      ticker.value = json
      if (json.last_price > 0) lastPrice.value = json.last_price
    } catch (e) {
      console.error('Failed to fetch ticker:', e)
    }
  }

  /** WebSocket 实时推送 */
  function connectWS() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return
    try {
      ws = new WebSocket(`${WS_BASE}/ws`)
      ws.onopen = () => {
        wsConnected.value = true
        // 订阅包含周期的精确主题
        ws?.send(JSON.stringify({ action: 'subscribe', topic: `klines.${symbol.value}.${timeframe.value}` }))
      }
      ws.onmessage = (event) => {
        const msg = JSON.parse(event.data)
        if (msg.topic?.startsWith('klines.') && msg.data) {
          const bar = msg.data as KlineData
          // 只接受与当前周期匹配的 bar（防止 1m bar 混入 1h 图表）
          const topicParts = msg.topic.split('.')
          const barTf = topicParts.length === 3 ? topicParts[2] : null
          if (barTf && barTf !== timeframe.value) return

          // 更新或追加最新 bar
          const last = klines.value[klines.value.length - 1]
          if (last && last.timestamp === bar.timestamp) {
            klines.value[klines.value.length - 1] = bar
          } else if (!last || bar.timestamp > last.timestamp) {
            klines.value.push(bar)
            if (klines.value.length > 300) klines.value.shift()
          }
          lastPrice.value = bar.close
          connected.value = true
        }
      }
      ws.onclose = () => {
        wsConnected.value = false
        // 3秒后重连
        reconnectTimer = setTimeout(connectWS, 3000)
      }
      ws.onerror = () => { ws?.close() }
    } catch (e) {
      wsConnected.value = false
    }
  }

  function disconnectWS() {
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null }
    ws?.close()
    ws = null
    wsConnected.value = false
  }

  /** 启动：REST加载历史 + WS实时 + Ticker轮询 */
  function start() {
    stop()
    fetchKlines()
    fetchTicker()
    connectWS()
    // Ticker 每 15 秒刷新
    tickerTimer = setInterval(fetchTicker, 15000)
    // REST 降级：如果 WS 断开，每 10 秒轮询 K 线
    pollTimer = setInterval(() => {
      if (!wsConnected.value) fetchKlines()
    }, 10000)
  }

  function stop() {
    disconnectWS()
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null }
    if (tickerTimer) { clearInterval(tickerTimer); tickerTimer = null }
  }

  function setSymbol(s: string) {
    symbol.value = s
    klines.value = []
    // 重新订阅 WS（包含新 symbol + 当前 timeframe）
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ action: 'subscribe', topic: `klines.${s}.${timeframe.value}` }))
    }
    fetchKlines()
    fetchTicker()
  }

  function setTimeframe(tf: string) {
    timeframe.value = tf
    klines.value = []
    // 重新订阅 WS（包含当前 symbol + 新 timeframe）
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ action: 'subscribe', topic: `klines.${symbol.value}.${tf}` }))
    }
    fetchKlines()
  }

  // 兼容旧接口
  function connect() { start() }
  function disconnect() { stop() }

  return {
    symbol, timeframe, klines, lastPrice, ticker, connected, loading, wsConnected,
    connect, disconnect, setSymbol, setTimeframe,
    fetchKlines, fetchTicker, start, stop,
  }
})
