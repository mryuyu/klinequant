<template>
  <div class="layout">
    <aside class="sidebar">
      <div class="logo">KlineQuant</div>
      <nav>
        <router-link to="/" class="nav-item">行情看板</router-link>
        <router-link to="/trade" class="nav-item">交易面板</router-link>
        <router-link to="/signals" class="nav-item">信号监控</router-link>
        <router-link to="/strategy" class="nav-item">策略管理</router-link>
        <router-link to="/backtest" class="nav-item">回测报告</router-link>
        <router-link to="/risk" class="nav-item">风控面板</router-link>
        <router-link to="/alerts" class="nav-item">告警中心</router-link>
        <router-link to="/system" class="nav-item">系统监控</router-link>
        <router-link to="/account" class="nav-item">账户总览</router-link>
        <router-link to="/settings" class="nav-item">系统设置</router-link>
      </nav>
      <div class="status">
        <span :class="market.connected ? 'dot-green' : 'dot-red'"></span>
        {{ market.connected ? '已连接' : '未连接' }}
      </div>
    </aside>
    <main class="content">
      <router-view />
    </main>
  </div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import { useMarketStore } from './stores/market'

const market = useMarketStore()

onMounted(() => {
  market.connect()
})
</script>

<style scoped>
.layout { display: flex; min-height: 100vh; }
.sidebar {
  width: 200px; background: var(--bg-secondary); border-right: 1px solid var(--border);
  padding: 20px 0; display: flex; flex-direction: column;
}
.logo { font-size: 20px; font-weight: 700; color: var(--accent); padding: 0 20px 20px; }
nav { flex: 1; }
.nav-item {
  display: block; padding: 12px 20px; color: var(--text-secondary);
  text-decoration: none; transition: all 0.2s;
}
.nav-item:hover, .nav-item.router-link-active { color: var(--accent); background: rgba(0,212,170,0.1); }
.status { padding: 12px 20px; font-size: 12px; color: var(--text-secondary); }
.dot-green, .dot-red { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.dot-green { background: var(--success); }
.dot-red { background: var(--danger); }
.content { flex: 1; padding: 24px; overflow-y: auto; }
</style>
