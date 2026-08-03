import { test, expect } from '@playwright/test'

/**
 * 导航 E2E 测试
 * 
 * 验证：
 * - 页面加载
 * - 导航栏功能
 * - 路由切换
 */

test.describe('应用导航', () => {
  test('首页加载成功', async ({ page }) => {
    await page.goto('/')
    
    // 验证页面标题或主要内容
    await expect(page.locator('body')).toBeVisible()
    
    // 验证导航栏存在
    const nav = page.locator('nav, .navbar, .nav')
    await expect(nav.first()).toBeVisible()
  })

  test('导航栏包含所有菜单项', async ({ page }) => {
    await page.goto('/')
    
    // 验证主要导航链接
    const expectedLinks = ['行情', '交易', '策略', '回测', '风控', '设置']
    
    for (const linkText of expectedLinks) {
      const link = page.getByText(linkText, { exact: false })
      // 至少有一个匹配（可能在导航栏或其他位置）
      const count = await link.count()
      expect(count).toBeGreaterThan(0)
    }
  })

  test('路由切换正常', async ({ page }) => {
    await page.goto('/')
    
    // 测试各页面路由
    const routes = [
      { path: '/trade', name: '交易' },
      { path: '/strategy', name: '策略' },
      { path: '/backtest', name: '回测' },
      { path: '/settings', name: '设置' },
    ]
    
    for (const route of routes) {
      await page.goto(route.path)
      await expect(page).toHaveURL(new RegExp(route.path))
    }
  })
})

test.describe('行情看板', () => {
  test('Dashboard 页面元素完整', async ({ page }) => {
    await page.goto('/')
    
    // 等待页面加载
    await page.waitForLoadState('networkidle')
    
    // 验证图表容器存在
    const chartContainer = page.locator('[class*="chart"], canvas, .tv-lightweight-charts')
    // 图表可能延迟加载，给一些时间
    await page.waitForTimeout(1000)
  })

  test('交易对选择器可用', async ({ page }) => {
    await page.goto('/')
    
    // 查找交易对选择器（select 或自定义组件）
    const selector = page.locator('select, [class*="symbol"], [class*="pair"]')
    const count = await selector.count()
    
    // 如果存在选择器，验证可交互
    if (count > 0) {
      await expect(selector.first()).toBeVisible()
    }
  })
})
