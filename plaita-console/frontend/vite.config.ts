import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
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

