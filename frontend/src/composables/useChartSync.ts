/**
 * 图表同步工具
 * 
 * 实现主图（K线）与副图（指标）之间的：
 * 1. 时间轴缩放/滚动同步
 * 2. 十字光标联动
 */
import type { IChartApi, ISeriesApi, SeriesType, MouseEventParams } from 'lightweight-charts'

interface ChartEntry {
  chart: IChartApi
  /** 用于十字光标定位的参考 series */
  series: ISeriesApi<SeriesType> | null
}

const charts = new Map<string, ChartEntry>()
let syncing = false
/** 记录已建立同步的图表实例对，避免重复订阅 */
const syncedPairs = new WeakSet<object>()

function pairKey(a: IChartApi, b: IChartApi): object {
  // 用两个 chart 实例组成的标记对象做 WeakSet key
  // 每次 register 都生成新 key，旧 chart 被 remove 后自然失效
  return [a, b]
}

/**
 * 注册图表实例（在 chart 创建后调用）
 * @param id 唯一标识，如 'main' / 'indicator'
 * @param chart lightweight-charts 实例
 * @param series 主 series（用于十字光标定位）
 */
export function registerChart(id: string, chart: IChartApi, series?: ISeriesApi<SeriesType> | null) {
  charts.set(id, { chart, series: series ?? null })
  setupSync()
}

/**
 * 更新图表的参考 series（指标切换时调用）
 */
export function updateSeries(id: string, series: ISeriesApi<SeriesType> | null) {
  const entry = charts.get(id)
  if (entry) entry.series = series
}

/**
 * 注销图表（在 chart.remove() 前调用）
 */
export function unregisterChart(id: string) {
  charts.delete(id)
}

function setupSync() {
  const entries = [...charts.entries()]
  if (entries.length < 2) return

  for (let i = 0; i < entries.length; i++) {
    for (let j = i + 1; j < entries.length; j++) {
      const [, entryA] = entries[i]
      const [, entryB] = entries[j]
      // 用当前 chart 实例检查是否已同步过
      const key = pairKey(entryA.chart, entryB.chart)
      if (syncedPairs.has(key)) continue
      syncedPairs.add(key)
      syncPair(entryA, entryB)
    }
  }
}

function syncPair(entryA: ChartEntry, entryB: ChartEntry) {
  const chartA = entryA.chart
  const chartB = entryB.chart

  // ─── 时间轴范围同步 ───
  chartA.timeScale().subscribeVisibleLogicalRangeChange((range) => {
    if (syncing || !range) return
    syncing = true
    try { chartB.timeScale().setVisibleLogicalRange(range) } catch { /* noop */ }
    syncing = false
  })

  chartB.timeScale().subscribeVisibleLogicalRangeChange((range) => {
    if (syncing || !range) return
    syncing = true
    try { chartA.timeScale().setVisibleLogicalRange(range) } catch { /* noop */ }
    syncing = false
  })

  // ─── 十字光标同步 ───
  chartA.subscribeCrosshairMove((param: MouseEventParams) => {
    if (syncing) return
    syncing = true
    try {
      syncCrosshair(param, chartB, entryB.series)
    } catch { /* noop */ }
    syncing = false
  })

  chartB.subscribeCrosshairMove((param: MouseEventParams) => {
    if (syncing) return
    syncing = true
    try {
      syncCrosshair(param, chartA, entryA.series)
    } catch { /* noop */ }
    syncing = false
  })
}

function syncCrosshair(param: MouseEventParams, targetChart: IChartApi, targetSeries: ISeriesApi<SeriesType> | null) {
  if (!targetSeries) return

  if (param.time && param.point) {
    // 获取当前图表中该 series 在对应时间的数据值
    const data = param.seriesData?.get(targetSeries as any)
    // 使用 logical 坐标定位更准确，但 setCrosshairPosition 需要 price 值
    // 如果无法获取精确值，使用 point.y 对应的 price
    const price = data ? extractPrice(data) : targetChart.timeScale().coordinateToTime(param.point.x)
    
    if (typeof price === 'number') {
      targetChart.setCrosshairPosition(price, param.time, targetSeries)
    }
  } else {
    targetChart.clearCrosshairPosition()
  }
}

function extractPrice(data: any): number | null {
  if (data == null) return null
  // Line/Histogram: { value: number }
  if (typeof data.value === 'number') return data.value
  // Candlestick: { open, high, low, close }
  if (typeof data.close === 'number') return data.close
  return null
}
