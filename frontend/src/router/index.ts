import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'dashboard', component: () => import('../views/DashboardView.vue') },
    { path: '/trade', name: 'trade', component: () => import('../views/TradeView.vue') },
    { path: '/signals', name: 'signals', component: () => import('../views/SignalView.vue') },
    { path: '/strategy', name: 'strategy', component: () => import('../views/StrategyView.vue') },
    { path: '/backtest', name: 'backtest', component: () => import('../views/BacktestView.vue') },
    { path: '/risk', name: 'risk', component: () => import('../views/RiskView.vue') },
    { path: '/alerts', name: 'alerts', component: () => import('../views/AlertView.vue') },
    { path: '/system', name: 'system', component: () => import('../views/SystemView.vue') },
    { path: '/account', name: 'account', component: () => import('../views/AccountView.vue') },
    { path: '/settings', name: 'settings', component: () => import('../views/SettingsView.vue') },
  ],
})

export default router
