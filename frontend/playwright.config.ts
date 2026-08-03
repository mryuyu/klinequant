import { defineConfig, devices } from '@playwright/test'

/**
 * KlineQuant E2E 测试配置
 * 
 * 运行方式：
 *   npx playwright test          # 运行所有测试
 *   npx playwright test --ui     # UI 模式
 *   npx playwright show-report   # 查看报告
 */
export default defineConfig({
  testDir: './e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  
  use: {
    baseURL: 'http://localhost:5175',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],

  /* 本地开发时自动启动前端服务器 */
  webServer: {
    command: 'npm run dev',
    url: 'http://localhost:5175',
    reuseExistingServer: true,
    timeout: 120 * 1000,
  },
})
