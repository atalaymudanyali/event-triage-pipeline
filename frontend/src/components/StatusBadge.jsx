const BADGE_CLASSES = new Set([
  'pending', 'processing', 'processed', 'failed',
  'low', 'medium', 'high', 'critical',
])

export default function StatusBadge({ value }) {
  if (!value) return null
  const cls = BADGE_CLASSES.has(value) ? `badge badge-${value}` : 'badge badge-default'
  return (
    <span className={cls}>
      {value}
    </span>
  )
}
