<template>
  <div class="indicator-panel">
    <div class="tab-bar">
      <button v-for="t in tabs" :key="t" :class="['tab', { active: active === t }]" @click="active = t">{{ t }}</button>
    </div>
    <div ref="container" class="sub-chart"></div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, onUnmounted } from 'vue'
import { createChart, LineSeries, HistogramSeries, type IChartApi, type ISeriesApi } from 'lightweight-charts'
import { useMarketStore } from '../stores/market'
import { registerChart, unregisterChart, updateSeries } from '../composables/useChartSync'

const market = useMarketStore()
const container = ref<HTMLElement>()
const tabs = ['MACD', 'RSI', 'KDJ']
const active = ref('MACD')

let chart: IChartApi | null = null
let series1: ISeriesApi<'Line'> | null = null
let series2: ISeriesApi<'Line'> | null = null
let series3: ISeriesApi<'Line'> | null = null
let histSeries: ISeriesApi<'Histogram'> | null = null

function initChart() {
  if (!container.value) return
  if (chart) {
    unregisterChart('indicator')
    chart.remove()
  }
  chart = createChart(container.value, {
    width: container.value.clientWidth,
    height: 150,
    layout: { background: { color: '#1a1a2e' }, textColor: '#a0a0a0' },
    grid: { vertLines: { color: '#2a2a4a' }, horzLines: { color: '#2a2a4a' } },
    crosshair: { mode: 0 },
    rightPriceScale: { scaleMargins: { top: 0.1, bottom: 0.1 } },
  })
  series1 = null; series2 = null; series3 = null; histSeries = null
  // 注册到图表同步系统（副图）
  registerChart('indicator', chart, null)
}

function renderIndicator() {
  if (!chart) initChart()
  if (!chart) return
  const bars = market.klines
  if (!bars.length) return

  // 清除旧 series
  if (series1) { chart.removeSeries(series1); series1 = null }
  if (series2) { chart.removeSeries(series2); series2 = null }
  if (series3) { chart.removeSeries(series3); series3 = null }
  if (histSeries) { chart.removeSeries(histSeries); histSeries = null }

  const closes = bars.map(b => b.close)
  const times = bars.map(b => (b.timestamp / 1000) as any)

  if (active.value === 'MACD') {
    const { dif, dea, macd } = calcMACD(closes)
    histSeries = chart.addSeries(HistogramSeries, { priceScaleId: 'left' })
    histSeries.setData(macd.map((v, i) => ({ time: times[i], value: v, color: v >= 0 ? 'rgba(81,207,102,0.6)' : 'rgba(255,107,107,0.6)' })))
    series1 = chart.addSeries(LineSeries, { color: '#f59e0b', lineWidth: 1 })
    series1.setData(dif.map((v, i) => ({ time: times[i], value: v })))
    series2 = chart.addSeries(LineSeries, { color: '#3b82f6', lineWidth: 1 })
    series2.setData(dea.map((v, i) => ({ time: times[i], value: v })))
    updateSeries('indicator', series1)
  } else if (active.value === 'RSI') {
    const rsi = calcRSI(closes, 14)
    series1 = chart.addSeries(LineSeries, { color: '#a855f7', lineWidth: 2 })
    series1.setData(rsi.map((v, i) => ({ time: times[i], value: v })))
    updateSeries('indicator', series1)
  } else if (active.value === 'KDJ') {
    const { k, d, j } = calcKDJ(bars.map(b => ({ high: b.high, low: b.low, close: b.close })))
    series1 = chart.addSeries(LineSeries, { color: '#f59e0b', lineWidth: 1 })
    series1.setData(k.map((v, i) => ({ time: times[i], value: v })))
    series2 = chart.addSeries(LineSeries, { color: '#3b82f6', lineWidth: 1 })
    series2.setData(d.map((v, i) => ({ time: times[i], value: v })))
    series3 = chart.addSeries(LineSeries, { color: '#a855f7', lineWidth: 1 })
    series3.setData(j.map((v, i) => ({ time: times[i], value: v })))
    updateSeries('indicator', series1)
  }
}

// ─── 指标计算 ───

function ema(data: number[], period: number): number[] {
  const result: number[] = []
  const k = 2 / (period + 1)
  let prev = data[0]
  for (let i = 0; i < data.length; i++) {
    prev = i === 0 ? data[0] : data[i] * k + prev * (1 - k)
    result.push(prev)
  }
  return result
}

function calcMACD(closes: number[], fast = 12, slow = 26, signal = 9) {
  const emaFast = ema(closes, fast)
  const emaSlow = ema(closes, slow)
  const dif = closes.map((_, i) => emaFast[i] - emaSlow[i])
  const dea = ema(dif, signal)
  const macd = dif.map((v, i) => (v - dea[i]) * 2)
  return { dif, dea, macd }
}

function calcRSI(closes: number[], period = 14): number[] {
  const rsi: number[] = new Array(closes.length).fill(50)
  let avgGain = 0, avgLoss = 0
  for (let i = 1; i < closes.length; i++) {
    const change = closes[i] - closes[i - 1]
    const gain = change > 0 ? change : 0
    const loss = change < 0 ? -change : 0
    if (i <= period) {
      avgGain += gain / period
      avgLoss += loss / period
    } else {
      avgGain = (avgGain * (period - 1) + gain) / period
      avgLoss = (avgLoss * (period - 1) + loss) / period
    }
    rsi[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss)
  }
  return rsi
}

function calcKDJ(bars: { high: number; low: number; close: number }[], period = 9) {
  const k: number[] = [], d: number[] = [], j: number[] = []
  let prevK = 50, prevD = 50
  for (let i = 0; i < bars.length; i++) {
    const start = Math.max(0, i - period + 1)
    let hh = -Infinity, ll = Infinity
    for (let x = start; x <= i; x++) {
      hh = Math.max(hh, bars[x].high)
      ll = Math.min(ll, bars[x].low)
    }
    const rsv = hh === ll ? 50 : ((bars[i].close - ll) / (hh - ll)) * 100
    const curK = (2 / 3) * prevK + (1 / 3) * rsv
    const curD = (2 / 3) * prevD + (1 / 3) * curK
    const curJ = 3 * curK - 2 * curD
    k.push(curK); d.push(curD); j.push(curJ)
    prevK = curK; prevD = curD
  }
  return { k, d, j }
}

watch([() => market.klines, active], () => { renderIndicator() }, { deep: true })
onMounted(() => { initChart(); renderIndicator() })
onUnmounted(() => {
  unregisterChart('indicator')
  chart?.remove()
})
</script>

<style scoped>
.indicator-panel { margin-top: 12px; }
.tab-bar { display: flex; gap: 4px; margin-bottom: 8px; }
.tab { padding: 4px 12px; font-size: 12px; border-radius: 4px; background: var(--bg-secondary); color: var(--text-secondary); border: 1px solid var(--border); cursor: pointer; }
.tab.active { background: rgba(0,212,170,0.15); color: var(--accent); border-color: var(--accent); }
.sub-chart { border-radius: 8px; overflow: hidden; }
</style>
