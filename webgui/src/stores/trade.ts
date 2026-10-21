import { defineStore } from 'pinia'
import { ref } from 'vue'

const API = 'http://localhost:8000'

export const useTradeStore = defineStore('trade', () => {
  const positions = ref<any[]>([])
  const orders = ref<any[]>([])
  const account = ref<any>(null)

  async function fetchPositions() {
    try {
      const resp = await fetch(`${API}/api/trade/positions`)
      const data = await resp.json()
      positions.value = data.positions || []
    } catch (e) { console.error(e) }
  }

  async function fetchOrders() {
    try {
      const resp = await fetch(`${API}/api/trade/orders`)
      const data = await resp.json()
      orders.value = data.orders || []
    } catch (e) { console.error(e) }
  }

  async function fetchAccount() {
    try {
      const resp = await fetch(`${API}/api/trade/account`)
      account.value = await resp.json()
    } catch (e) { console.error(e) }
  }

  async function submitOrder(order: any) {
    try {
      const resp = await fetch(`${API}/api/trade/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(order),
      })
      return await resp.json()
    } catch (e) { console.error(e); return null }
  }

  return { positions, orders, account, fetchPositions, fetchOrders, fetchAccount, submitOrder }
})
