<template>
  <div>
    <h2>账户总览</h2>
    <div class="cards">
      <div class="card stat">
        <div class="label">总权益 (USDT)</div>
        <div class="value">{{ fmt(account?.total_balance) }}</div>
      </div>
      <div class="card stat">
        <div class="label">可用余额</div>
        <div class="value">{{ fmt(account?.available_balance) }}</div>
      </div>
      <div class="card stat">
        <div class="label">未实现盈亏</div>
        <div class="value" :class="(account?.unrealized_pnl || 0) >= 0 ? 'text-green' : 'text-red'">{{ fmt(account?.unrealized_pnl) }}</div>
      </div>
    </div>

    <!-- 资产列表 -->
    <div class="card" style="margin-top: 20px;">
      <h3>资产明细</h3>
      <table>
        <thead><tr><th>币种</th><th>可用</th><th>冻结</th><th>合计</th></tr></thead>
        <tbody>
          <tr v-for="a in account?.assets || []" :key="a.asset">
            <td class="mono">{{ a.asset }}</td>
            <td>{{ fmtNum(a.free) }}</td>
            <td>{{ fmtNum(a.locked) }}</td>
            <td class="bold">{{ fmtNum(a.free + a.locked) }}</td>
          </tr>
          <tr v-if="!(account?.assets || []).length"><td colspan="4" class="text-muted">暂无资产数据</td></tr>
        </tbody>
      </table>
    </div>

    <!-- 持仓 -->
    <div class="card" style="margin-top: 20px;">
      <h3>当前持仓</h3>
      <table>
        <thead><tr><th>品种</th><th>方向</th><th>数量</th><th>可用</th><th>冻结</th></tr></thead>
        <tbody>
          <tr v-for="p in positions" :key="p.symbol">
            <td class="mono">{{ p.symbol }}</td>
            <td class="text-green">{{ p.side }}</td>
            <td>{{ fmtNum(p.quantity) }}</td>
            <td>{{ fmtNum(p.free) }}</td>
            <td>{{ fmtNum(p.locked) }}</td>
          </tr>
          <tr v-if="!positions.length"><td colspan="5" class="text-muted">暂无持仓</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, computed } from 'vue'
import { useTradeStore } from '../stores/trade'

const trade = useTradeStore()
const account = computed(() => trade.account)
const positions = computed(() => trade.positions)

function fmt(v: number | undefined): string {
  if (v === undefined || v === null) return '--'
  return v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}
function fmtNum(v: number): string {
  if (!v) return '0'
  return v.toLocaleString(undefined, { maximumFractionDigits: 8 })
}

onMounted(() => {
  trade.fetchAccount()
  trade.fetchPositions()
})
</script>

<style scoped>
h2 { margin-bottom: 16px; }
h3 { margin-bottom: 12px; font-size: 14px; }
.cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
.stat { text-align: center; padding: 20px; }
.label { font-size: 12px; color: var(--text-secondary); margin-bottom: 8px; }
.value { font-size: 24px; font-weight: 700; font-family: var(--font-mono); }
table { width: 100%; font-size: 13px; border-collapse: collapse; }
th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--border); }
th { color: var(--text-secondary); font-weight: 500; font-size: 12px; }
.mono { font-family: var(--font-mono); }
.bold { font-weight: 600; }
.text-green { color: var(--success); }
.text-red { color: var(--danger); }
.text-muted { color: var(--text-secondary); text-align: center; }
</style>
