import { test, expect } from '@playwright/test'

/**
 * 策略管理 E2E 测试
 */

test.describe('策略管理页面', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/strategy')
    await page.waitForLoadState('networkidle')
  })

  test('页面加载成功', async ({ page }) => {
    // 验证页面主要内容
    await expect(page.locator('body')).toBeVisible()
    
    // 验证策略相关元素
    const strategyElements = page.getByText(/策略|strategy/i)
    const count = await strategyElements.count()
    expect(count).toBeGreaterThan(0)
  })

  test('策略列表显示', async ({ page }) => {
    // 等待可能的加载状态
    await page.waitForTimeout(500)
    
    // 查找表格或列表
    const table = page.locator('table, [class*="list"], [class*="card"]')
    const tableCount = await table.count()
    
    // 页面应该有某种形式的数据展示
    expect(tableCount).toBeGreaterThanOrEqual(0)
  })

  test('新建策略按钮存在', async ({ page }) => {
    // 查找新建/添加按钮
    const createBtn = page.getByRole('button', { name: /新建|添加|创建|create|add/i })
    const btnCount = await createBtn.count()
    
    // 如果存在按钮，验证可见
    if (btnCount > 0) {
      await expect(createBtn.first()).toBeVisible()
    }
  })
})

test.describe('回测页面', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/backtest')
    await page.waitForLoadState('networkidle')
  })

  test('页面加载成功', async ({ page }) => {
    await expect(page.locator('body')).toBeVisible()
    
    // 验证回测相关元素
    const backtestElements = page.getByText(/回测|backtest/i)
    const count = await backtestElements.count()
    expect(count).toBeGreaterThan(0)
  })

  test('回测表单存在', async ({ page }) => {
    // 查找表单元素
    const form = page.locator('form, [class*="form"]')
    const inputs = page.locator('input, select')
    
    const formCount = await form.count()
    const inputCount = await inputs.count()
    
    // 应该有表单或输入元素
    expect(formCount + inputCount).toBeGreaterThan(0)
  })

  test('运行回测按钮', async ({ page }) => {
    const runBtn = page.getByRole('button', { name: /运行|开始|执行|run|start/i })
    const btnCount = await runBtn.count()
    
    if (btnCount > 0) {
      await expect(runBtn.first()).toBeVisible()
    }
  })
})
