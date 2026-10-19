import React from 'react'
import ReactDOM from 'react-dom/client'
import { MutationCache, QueryClient, QueryClientProvider } from '@tanstack/react-query'
// 自托管字体（混合式排版：UI=Inter，数据=JetBrains Mono，见 DESIGN.md §1）
import '@fontsource-variable/inter'
import '@fontsource-variable/jetbrains-mono'
import App from './App'
import './index.css'
import { shouldRetryQuery, errorUserMessage } from './services/transport'
import { emitApiError } from './components/ui/apiErrorBus'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      // S17-03：最多重试 1 次，仅超时/网络/408/429/5xx；401/403/404/409/422 不重试
      retry: shouldRetryQuery,
      staleTime: 5000,
    },
    mutations: {
      // 写操作超时可能已在服务端成功，禁止自动重放
      retry: 0,
    },
  },
  // 兜底：没有任何 onError 的写操作也不静默（meta.silent 可豁免）
  mutationCache: new MutationCache({
    onError: (error, _variables, _context, mutation) => {
      if (mutation.meta?.silent) return
      const message = errorUserMessage(error)
      console.warn('[api-mutation] 写操作失败:', message, error)
      emitApiError(message)
    },
  }),
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </React.StrictMode>,
)

