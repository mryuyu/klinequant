<template>
  <div class="orderbook">
    <h4>盘口</h4>
    <div class="ob-section asks">
      <div v-for="(a, i) in asks" :key="'a'+i" class="ob-row ask">
        <span class="price">{{ a.price.toLocaleString() }}</span>
        <span class="qty">{{ a.qty.toFixed(4) }}</span>
        <div class="depth-bar ask-bar" :style="{ width: barWidth(a.qty) }"></div>
      </div>
    </div>
    <div class="ob-mid">
      <span class="spread">{{ spread }}</span>
    </div>
    <div class="ob-section bids">
      <div v-for="(b, i) in bids" :key="'b'+i" class="ob-row bid">
        <span class="price">{{ b.price.toLocaleString() }}</span>
        <span class="qty">{{ b.qty.toFixed(4) }}</span>
        <div class="depth-bar bid-bar" :style="{ width: barWidth(b.qty) }"></div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onUnmounted } from 'vue'
import { useMarketStore } from '../stores/market'

const market = useMarketStore()
const API = 'http://localhost:8000'

interface Level { price: number; qty: number }
const bids = ref<Level[]>([])
const asks = ref<Level[]>([])
let timer: ReturnType<typeof setInterval> | null = null

const maxQty = computed(() => {
  const all = [...bids.value, ...asks.value]
  return all.length ? Math.max(...all.map(l => l.qty)) : 1
})

const spread = computed(() => {
  if (!asks.value.length || !bids.value.length) return '-'
  const s = asks.value[0].price - bids.value[0].price
  return `价差 ${s.toFixed(2)}`
})

function barWidth(qty: number): string {
  return `${Math.min((qty / maxQty.value) * 100, 100)}%`
}

async function fetchDepth() {
  try {
    const resp = await fetch(`${API}/api/market/depth?symbol=${market.symbol}&limit=5`)
    const data = await resp.json()
    bids.value = data.bids || []
    asks.value = (data.asks || []).reverse() // 卖盘从低到高显示
  } catch (e) { /* ignore */ }
}

watch(() => market.symbol, () => { fetchDepth() })
onMounted(() => { fetchDepth(); timer = setInterval(fetchDepth, 3000) })
onUnmounted(() => { if (timer) clearInterval(timer) })
</script>

<style scoped>
.orderbook { font-size: 12px; font-family: var(--font-mono); }
h4 { font-size: 13px; margin-bottom: 8px; font-family: var(--font-sans); }
.ob-row { position: relative; display: flex; justify-content: space-between; padding: 2px 8px; }
.ob-row .price { z-index: 1; }
.ob-row .qty { z-index: 1; color: var(--text-secondary); }
.ask .price { color: #ff6b6b; }
.bid .price { color: #51cf66; }
.depth-bar { position: absolute; top: 0; right: 0; height: 100%; opacity: 0.12; }
.ask-bar { background: #ff6b6b; }
.bid-bar { background: #51cf66; }
.ob-mid { text-align: center; padding: 4px; color: var(--text-secondary); font-size: 11px; border-top: 1px solid var(--border); border-bottom: 1px solid var(--border); margin: 4px 0; }
</style>
