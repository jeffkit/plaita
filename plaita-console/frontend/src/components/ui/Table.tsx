import type { HTMLAttributes, TdHTMLAttributes, ThHTMLAttributes } from 'react'
import { cn } from './cn'

/** 数据表基元组（DESIGN.md §5）：micro 大写表头、行分隔 line、hover elevated */

export function Table({ className, children, ...rest }: HTMLAttributes<HTMLTableElement>) {
  return (
    <div className="overflow-x-auto">
      <table className={cn('w-full text-body', className)} {...rest}>
        {children}
      </table>
    </div>
  )
}

export function Th({ className, ...rest }: ThHTMLAttributes<HTMLTableCellElement>) {
  return (
    <th
      className={cn(
        'text-left font-medium py-2 px-3 text-micro uppercase text-ink-muted border-b border-line',
        className,
      )}
      {...rest}
    />
  )
}

export function Tr({ className, ...rest }: HTMLAttributes<HTMLTableRowElement>) {
  return (
    <tr
      className={cn('border-b border-line hover:bg-elevated/40 transition-colors', className)}
      {...rest}
    />
  )
}

export function Td({ className, ...rest }: TdHTMLAttributes<HTMLTableCellElement>) {
  return <td className={cn('py-2.5 px-3 text-ink-secondary align-middle', className)} {...rest} />
}

/** 数据声道单元格：执行 ID / 时间戳等（mono + 次要色） */
export function TdData({ className, ...rest }: TdHTMLAttributes<HTMLTableCellElement>) {
  return <Td className={cn('font-mono text-data-sm', className)} {...rest} />
}
