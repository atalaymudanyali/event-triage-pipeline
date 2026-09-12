import { useState } from 'react'

export default function TicketForm({ onSubmit }) {
  const [form, setForm] = useState({
    customer_name: '',
    customer_email: '',
    subject: '',
    message: '',
  })
  const [submitting, setSubmitting] = useState(false)

  const handleChange = (e) => {
    setForm({ ...form, [e.target.name]: e.target.value })
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setSubmitting(true)
    try {
      await onSubmit(form)
      setForm({ customer_name: '', customer_email: '', subject: '', message: '' })
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <form className="ticket-form" onSubmit={handleSubmit}>
      <div className="form-row">
        <input
          name="customer_name"
          placeholder="Your name"
          value={form.customer_name}
          onChange={handleChange}
          required
        />
        <input
          name="customer_email"
          type="email"
          placeholder="Your email"
          value={form.customer_email}
          onChange={handleChange}
          required
        />
      </div>
      <input
        name="subject"
        placeholder="Subject"
        value={form.subject}
        onChange={handleChange}
        required
      />
      <textarea
        name="message"
        placeholder="Describe your issue..."
        value={form.message}
        onChange={handleChange}
        rows={4}
        required
      />
      <button type="submit" disabled={submitting}>
        {submitting ? 'Submitting...' : 'Submit Ticket'}
      </button>
    </form>
  )
}
