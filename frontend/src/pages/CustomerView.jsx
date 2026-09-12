import { useState } from 'react'
import { api } from '../api'
import TicketDetail from '../components/TicketDetail'
import TicketForm from '../components/TicketForm'

export default function CustomerView() {
  const [submitted, setSubmitted] = useState(null)
  const [lookupId, setLookupId] = useState('')
  const [lookedUp, setLookedUp] = useState(null)
  const [error, setError] = useState('')

  const handleSubmit = async (data) => {
    setError('')
    try {
      const ticket = await api.createTicket(data)
      setSubmitted(ticket)
    } catch (e) {
      setError(e.message)
    }
  }

  const handleLookup = async (e) => {
    e.preventDefault()
    setError('')
    setLookedUp(null)
    try {
      const ticket = await api.getTicket(lookupId.trim())
      setLookedUp(ticket)
    } catch (e) {
      setError(e.message)
    }
  }

  return (
    <div className="customer-view">
      <h1>Submit a Support Ticket</h1>
      <TicketForm onSubmit={handleSubmit} />

      {submitted && (
        <div className="success-msg">
          Ticket submitted! Your ticket ID: <code>{submitted.event_id}</code>
        </div>
      )}

      {error && <div className="error-msg">{error}</div>}

      <hr />

      <h2>Check Ticket Status</h2>
      <form className="lookup-form" onSubmit={handleLookup}>
        <input
          placeholder="Enter your ticket ID"
          value={lookupId}
          onChange={(e) => setLookupId(e.target.value)}
          required
        />
        <button type="submit">Look Up</button>
      </form>

      {lookedUp && <TicketDetail ticket={lookedUp} />}
    </div>
  )
}
