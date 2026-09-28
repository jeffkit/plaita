import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  // 相对资源路径：dsht 隧道以 /plaita/ 子路径（剥前缀）暴露，根路径部署同样可用。
  // 注意：客户端子路由深链直刷（如 /plaita/executions/x 刷新）资源解析会退化，
  // 需回到 /plaita/ 入口——客户端路由跳转不受影响。
  base: './',
  server: {
    port: Number(process.env.PLAITA_CONSOLE_FRONTEND_PORT) || 5173,
    proxy: {
      '/api': {
        // 后端地址可用环境变量覆盖（多实例/非默认端口本地开发，
        // 如 PLAITA_CONSOLE_API_TARGET=http://localhost:8090）
        target: process.env.PLAITA_CONSOLE_API_TARGET || 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
})

