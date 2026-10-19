/** 全局 API 错误事件总线（MutationCache.onError → ApiErrorBanner）。 */
export type ApiErrorListener = (message: string) => void

const listeners = new Set<ApiErrorListener>()

export function emitApiError(message: string): void {
  for (const listener of listeners) listener(message)
}

export function subscribeApiErrors(listener: ApiErrorListener): () => void {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}
