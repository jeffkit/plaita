/** 极简 className 合并：过滤假值后拼接（项目未引入 clsx，保持零依赖） */
export function cn(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(' ')
}
