import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
// base：dev（vite serve）用 '/'，保持 `npm run dev` 于 :5174/ 根路径不变；
// build 用 '/app/'，产物由 gateway 托管在 http://127.0.0.1:8000/app/（与 lc-live.html 首页 / 共存）。
export default defineConfig(({ command }) => ({
  base: command === 'build' ? '/app/' : '/',
  plugins: [vue()],
}))
