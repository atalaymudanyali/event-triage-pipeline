import { Fragment, useState } from 'react'
import StatusBadge from './StatusBadge'
import TicketDetail from './TicketDetail'

export default function TicketTable({ tickets, onProcess, processingIds }) {
  const [expandedId, setExpandedId] = useState(null)

  return (
    <div className="ticket-table-wrapper">
      <table className="ticket-table">
        <thead>
          <tr>
            <th>Customer</th>
            <th>Subject</th>
            <th>Status</th>
            <th>Category</th>
            <th>Urgency</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {tickets.map((t) => (
            <Fragment key={t.event_id}>
              <tr
                className={expandedId === t.event_id ? 'row-expanded' : ''}
                onClick={() => setExpandedId(expandedId === t.event_id ? null : t.event_id)}
              >
                <td>{t.customer_name}</td>
                <td>{t.subject}</td>
                <td><StatusBadge value={t.status} /></td>
                <td>{t.category ? <StatusBadge value={t.category} /> : '—'}</td>
                <td>{t.urgency ? <StatusBadge value={t.urgency} /> : '—'}</td>
                <td>
                  {t.status === 'pending' && (
                    <button
                      className="btn-sm"
                      disabled={processingIds.has(t.event_id)}
                      onClick={(e) => { e.stopPropagation(); onProcess(t.event_id) }}
                    >
                      {processingIds.has(t.event_id) ? 'Processing...' : 'Process'}
                    </button>
                  )}
                  {t.status === 'processing' && <span className="processing-text">Processing...</span>}
                </td>
              </tr>
              {expandedId === t.event_id && (
                <tr>
                  <td colSpan={6}>
                    <TicketDetail ticket={t} />
                  </td>
                </tr>
              )}
            </Fragment>
          ))}
        </tbody>
      </table>
      {tickets.length === 0 && <p className="empty-state">No tickets yet</p>}
    </div>
  )
}
