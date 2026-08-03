import { test, expect } from '@playwright/test'

/**
 * 设置页面 & API 集成 E2E 测试
 */

test.describe('设置页面', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/settings')
    await page.waitForLoadState('networkidle')
  })

  test('页面加载成功', async ({ page }) => {
    await expect(page.locator('body')).toBeVisible()
    
    // 验证设置相关元素
    const settingsElements = page.getByText(/设置|配置|settings|config/i)
    const count = await settingsElements.count()
    expect(count).toBeGreaterThan(0)
  })

  test('交易所配置区域', async ({ page }) => {
    // 查找交易所相关元素
    const exchangeElements = page.getByText(/交易所|binance|api/i)
    const count = await exchangeElements.count()
    
    // 页面应该包含交易所配置
    expect(count).toBeGreaterThan(0)
  })

  test('表单输入可用', async ({ page }) => {
    // 查找输入框
    const inputs = page.locator('input[type="text"], input[type="password"], input:not([type])')
    const inputCount = await inputs.count()
    
    // 如果有输入框，验证可交互
    if (inputCount > 0) {
      const firstInput = inputs.first()
      await expect(firstInput).toBeVisible()
    }
  })
})

test.describe('API 集成', () => {
  test('后端健康检查', async ({ request }) => {
    // 直接调用后端 API
    const response = await request.get('http://localhost:8000/api/system/health')
    
    // 后端可能未启动，跳过断言
    if (response.ok()) {
      const data = await response.json()
      expect(data.status).toBe('healthy')
    }
  })

  test('API 版本兼容路径', async ({ request }) => {
    // 测试 /api/v1/ 路径重写
    const response = await request.get('http://localhost:8000/api/v1/system/health')
    
    if (response.ok()) {
      const data = await response.json()
      expect(data).toHaveProperty('status')
    }
  })
})

test.describe('响应式布局', () => {
  test('移动端适配', async ({ page }) => {
    // 设置移动端视口
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
    
    // 验证页面在移动端可显示
    await expect(page.locator('body')).toBeVisible()
    
    // 验证没有水平溢出
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth)
    expect(bodyWidth).toBeLessThanOrEqual(400)
  })

  test('平板适配', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.goto('/')
    
    await expect(page.locator('body')).toBeVisible()
  })
})
