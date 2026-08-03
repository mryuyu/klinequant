<template>
  <div ref="chartContainer" class="chart-container"></div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue'
import { createChart, CandlestickSeries, HistogramSeries, type IChartApi, type ISeriesApi } from 'lightweight-charts'
import { useMarketStore } from '../stores/market'
import { registerChart, unregisterChart } from '../composables/useChartSync'

const chartContainer = ref<HTMLElement>()
const market = useMarketStore()
let chart: IChartApi | null = null
let candleSeries: ISeriesApi<'Candlestick'> | null = null
let volumeSeries: ISeriesApi<'Histogram'> | null = null

onMounted(() => {
  if (!chartContainer.value) return
  chart = createChart(chartContainer.value, {
    width: chartContainer.value.clientWidth,
    height: 400,
    layout: { background: { color: '#1a1a2e' }, textColor: '#a0a0a0' },
    grid: { vertLines: { color: '#2a2a4a' }, horzLines: { color: '#2a2a4a' } },
    crosshair: { mode: 0 },
  })
  candleSeries = chart.addSeries(CandlestickSeries, {
    upColor: '#51cf66', downColor: '#ff6b6b',
    borderUpColor: '#51cf66', borderDownColor: '#ff6b6b',
    wickUpColor: '#51cf66', wickDownColor: '#ff6b6b',
  })
  volumeSeries = chart.addSeries(HistogramSeries, {
    priceFormat: { type: 'volume' },
    priceScaleId: 'volume',
  })
  chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } })

  // 注册到图表同步系统（主图）
  registerChart('main', chart, candleSeries)
})

watch(() => market.klines, (bars) => {
  if (!candleSeries || !bars.length) return
  candleSeries.setData(bars.map(b => ({
    time: (b.timestamp / 1000) as any,
    open: b.open, high: b.high, low: b.low, close: b.close,
  })))
  volumeSeries?.setData(bars.map(b => ({
    time: (b.timestamp / 1000) as any,
    value: b.volume,
    color: b.close >= b.open ? 'rgba(81,207,102,0.3)' : 'rgba(255,107,107,0.3)',
  })))
}, { deep: true })

onUnmounted(() => {
  unregisterChart('main')
  chart?.remove()
})
</script>

<style scoped>
.chart-container { width: 100%; border-radius: 8px; overflow: hidden; }
</style>
