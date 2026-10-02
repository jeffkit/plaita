import { useEffect, useState } from 'react'

/** 值静默 delay 毫秒后才落地（C5-3 拖拽热路径优化共享原语）：
 *  拖拽/连线的连续变更期间定时器不断重置，停止后只落地一次——
 *  依赖它的 useMemo/useRef 计算从「每帧一次」降为「每突发一次」。 */
export function useDebouncedValue<T>(value: T, delay = 300): T {
  const [snapped, setSnapped] = useState(value)
  useEffect(() => {
    const t = setTimeout(() => setSnapped(value), delay)
    return () => clearTimeout(t)
  }, [value, delay])
  return snapped
}
