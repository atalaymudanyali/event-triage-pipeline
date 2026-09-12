import StatusBadge from './StatusBadge'

export default function TicketDetail({ ticket }) {
  if (!ticket) return null

  return (
    <div className="ticket-detail">
      <h3>{ticket.subject}</h3>
      <p className="ticket-meta">
        {ticket.customer_name} &middot; {ticket.customer_email}
      </p>
      <p className="ticket-message">{ticket.message}</p>

      <div className="ticket-badges">
        <StatusBadge value={ticket.status} />
        {ticket.category && <StatusBadge value={ticket.category} />}
        {ticket.urgency && <StatusBadge value={ticket.urgency} />}
        {ticket.suggested_action && <StatusBadge value={ticket.suggested_action} />}
      </div>

      {ticket.draft_response && (
        <div className="draft-response">
          <strong>Draft Response:</strong>
          <p>{ticket.draft_response}</p>
        </div>
      )}

      {ticket.reasoning && (
        <div className="reasoning">
          <strong>Reasoning:</strong>
          <p>{ticket.reasoning}</p>
        </div>
      )}

      <p className="ticket-id">ID: {ticket.event_id}</p>
    </div>
  )
}
