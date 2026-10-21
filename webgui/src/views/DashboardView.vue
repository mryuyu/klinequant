<template>
  <div>
    <h2>行情看板</h2>
    <div class="header-row">
      <select v-model="market.symbol" @change="market.setSymbol(market.symbol)" class="select">
        <option value="BTCUSDT">BTC/USDT</option>
        <option value="ETHUSDT">ETH/USDT</option>
        <option value="BNBUSDT">BNB/USDT</option>
        <option value="SOLUSDT">SOL/USDT</option>
        <option value="XRPUSDT">XRP/USDT</option>
        <option value="DOGEUSDT">DOGE/USDT</option>
      </select>
      <select v-model="market.timeframe" @change="market.setTimeframe(market.timeframe)" class="select">
        <option value="1m">1分</option>
        <option value="5m">5分</option>
        <option value="15m">15分</option>
        <option value="1h">1时</option>
        <option value="4h">4时</option>
        <option value="1d">1日</option>
        <option value="1w">1周</option>
      </select>
      <span class="price" :class="priceColor">
        {{ market.lastPrice ? market.lastPrice.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '--' }}
      </span>
      <span v-if="market.ticker" class="change" :class="market.ticker.price_change_pct >= 0 ? 'text-green' : 'text-red'">
        {{ market.ticker.price_change_pct >= 0 ? '+' : '' }}{{ market.ticker.price_change_pct?.toFixed(2) }}%
      </span>
      <span class="conn-badge" :class="market.connected ? 'online' : 'offline'">
        {{ market.connected ? '● 已连接' : '○ 断开' }}
      </span>
    </div>

    <!-- 24h 行情摘要 -->
    <div v-if="market.ticker" class="ticker-row">
      <div class="ticker-item">
        <span class="label">24h 最高</span>
        <span class="value">{{ market.ticker.high_24h?.toLocaleString() }}</span>
      </div>
      <div class="ticker-item">
        <span class="label">24h 最低</span>
        <span class="value">{{ market.ticker.low_24h?.toLocaleString() }}</span>
      </div>
      <div class="ticker-item">
        <span class="label">24h 成交量</span>
        <span class="value">{{ formatVolume(market.ticker.volume_24h) }}</span>
      </div>
      <div class="ticker-item">
        <span class="label">买一</span>
        <span class="value text-green">{{ market.ticker.bid?.toLocaleString() }}</span>
      </div>
      <div class="ticker-item">
        <span class="label">卖一</span>
        <span class="value text-red">{{ market.ticker.ask?.toLocaleString() }}</span>
      </div>
    </div>

    <div class="chart-layout">
      <div class="card chart-main">
        <KlineChart />
        <IndicatorPanel />
      </div>
      <div class="card chart-side">
        <OrderBook />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useMarketStore } from '../stores/market'
import KlineChart from '../components/KlineChart.vue'
import IndicatorPanel from '../components/IndicatorPanel.vue'
import OrderBook from '../components/OrderBook.vue'

const market = useMarketStore()

const priceColor = computed(() => {
  if (!market.ticker) return ''
  return market.ticker.price_change_pct >= 0 ? 'text-green' : 'text-red'
})

function formatVolume(v: number): string {
  if (v >= 1e9) return (v / 1e9).toFixed(2) + 'B'
  if (v >= 1e6) return (v / 1e6).toFixed(2) + 'M'
  if (v >= 1e3) return (v / 1e3).toFixed(2) + 'K'
  return v.toFixed(2)
}

// App.vue 已通过 market.connect() 启动数据流，此处无需重复启动
</script>

<style scoped>
h2 { margin-bottom: 16px; }
.header-row { display: flex; align-items: center; gap: 16px; margin-bottom: 12px; flex-wrap: wrap; }
.select { background: var(--bg-secondary); color: var(--text-primary); border: 1px solid var(--border); padding: 8px 12px; border-radius: 6px; }
.price { font-size: 24px; font-weight: 700; font-family: var(--font-mono); }
.change { font-size: 14px; font-weight: 600; }
.text-green { color: #51cf66; }
.text-red { color: #ff6b6b; }
.conn-badge { font-size: 12px; margin-left: auto; }
.conn-badge.online { color: #51cf66; }
.conn-badge.offline { color: #ff6b6b; }

.ticker-row {
  display: flex; gap: 24px; margin-bottom: 16px; padding: 12px 16px;
  background: var(--bg-secondary); border-radius: 8px; flex-wrap: wrap;
}
.ticker-item { display: flex; flex-direction: column; gap: 2px; }
.ticker-item .label { font-size: 11px; color: var(--text-secondary); }
.ticker-item .value { font-size: 14px; font-family: var(--font-mono); font-weight: 600; }

.chart-layout { display: grid; grid-template-columns: 1fr 220px; gap: 16px; }
.chart-side { padding: 12px; }
@media (max-width: 900px) { .chart-layout { grid-template-columns: 1fr; } }
</style>
