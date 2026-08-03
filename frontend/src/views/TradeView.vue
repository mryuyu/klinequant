<template>
  <div>
    <h2>交易面板</h2>
    <div class="grid">
      <div class="card">
        <h3>手动下单</h3>
        <div class="form">
          <select v-model="order.symbol" class="input"><option>BTCUSDT</option><option>ETHUSDT</option></select>
          <div class="row">
            <button class="btn btn-success" @click="order.side='BUY'" :class="{active: order.side==='BUY'}">买入</button>
            <button class="btn btn-danger" @click="order.side='SELL'" :class="{active: order.side==='SELL'}">卖出</button>
          </div>
          <input v-model="order.quantity" type="number" placeholder="数量" class="input" />
          <input v-model="order.price" type="number" placeholder="价格(限价)" class="input" />
          <button class="btn btn-primary" @click="submit">提交订单</button>
        </div>
      </div>
      <div class="card">
        <h3>当前持仓</h3>
        <table><thead><tr><th>品种</th><th>方向</th><th>数量</th><th>盈亏</th></tr></thead>
        <tbody><tr v-for="p in trade.positions" :key="p.symbol">
          <td>{{ p.symbol }}</td><td>{{ p.side }}</td><td>{{ p.quantity }}</td>
          <td :class="p.pnl >= 0 ? 'text-green' : 'text-red'">{{ p.pnl?.toFixed(2) }}</td>
        </tr><tr v-if="!trade.positions.length"><td colspan="4" class="text-muted">暂无持仓</td></tr></tbody></table>
      </div>
      <div class="card">
        <h3>挂单</h3>
        <table><thead><tr><th>品种</th><th>方向</th><th>数量</th><th>价格</th><th>状态</th></tr></thead>
        <tbody><tr v-for="o in trade.orders" :key="o.order_id">
          <td>{{ o.symbol }}</td><td>{{ o.side }}</td><td>{{ o.quantity }}</td><td>{{ o.price }}</td><td>{{ o.status }}</td>
        </tr><tr v-if="!trade.orders.length"><td colspan="5" class="text-muted">暂无挂单</td></tr></tbody></table>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { reactive, onMounted } from 'vue'
import { useTradeStore } from '../stores/trade'
const trade = useTradeStore()
const order = reactive({ symbol: 'BTCUSDT', side: 'BUY', quantity: 0.01, price: null as number | null })
async function submit() {
  await trade.submitOrder({ symbol: order.symbol, side: order.side, quantity: order.quantity, price: order.price, order_type: order.price ? 'LIMIT' : 'MARKET' })
  await trade.fetchOrders()
}
onMounted(() => { trade.fetchPositions(); trade.fetchOrders() })
</script>

<style scoped>
h2 { margin-bottom: 16px; } h3 { margin-bottom: 12px; font-size: 14px; }
.grid { display: grid; grid-template-columns: 300px 1fr; gap: 16px; }
.form { display: flex; flex-direction: column; gap: 10px; }
.input { background: var(--bg-primary); border: 1px solid var(--border); color: var(--text-primary); padding: 8px 12px; border-radius: 6px; }
.row { display: flex; gap: 8px; }
.active { outline: 2px solid var(--accent); }
</style>
