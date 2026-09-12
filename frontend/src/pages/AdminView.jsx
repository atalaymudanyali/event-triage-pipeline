import { useCallback, useEffect, useState } from 'react'
import { api } from '../api'
import TicketTable from '../components/TicketTable'

export default function AdminView() {
  const [tickets, setTickets] = useState([])
  const [stats, setStats] = useState(null)
  const [processingIds, setProcessingIds] = useState(new Set())
  const [generating, setGenerating] = useState(false)
  const [error, setError] = useState('')

  const refresh = useCallback(async () => {
    try {
      const [ticketList, statsData] = await Promise.all([
        api.listTickets(),
        api.getStats(),
      ])
      setTickets(ticketList)
      setStats(statsData)
    } catch (e) {
      setError(e.message)
    }
  }, [])

  useEffect(() => { refresh() }, [refresh])

  const handleGenerate = async () => {
    setGenerating(true)
    setError('')
    try {
      await api.generateTicket()
      await refresh()
    } catch (e) {
      setError(e.message)
    } finally {
      setGenerating(false)
    }
  }

  const handleProcess = async (eventId) => {
    setProcessingIds((prev) => new Set(prev).add(eventId))
    setError('')
    try {
      await api.processTicket(eventId)
      await refresh()
    } catch (e) {
      setError(e.message)
    } finally {
      setProcessingIds((prev) => {
        const next = new Set(prev)
        next.delete(eventId)
        return next
      })
    }
  }

  return (
    <div className="admin-view">
      <h1>Admin Dashboard</h1>

      {stats && (
        <div className="stat-cards">
          <div className="stat-card">
            <span className="stat-value">{stats.total}</span>
            <span className="stat-label">Total</span>
          </div>
          <div className="stat-card stat-pending">
            <span className="stat-value">{stats.pending}</span>
            <span className="stat-label">Pending</span>
          </div>
          <div className="stat-card stat-processed">
            <span className="stat-value">{stats.processed}</span>
            <span className="stat-label">Processed</span>
          </div>
          <div className="stat-card stat-failed">
            <span className="stat-value">{stats.failed}</span>
            <span className="stat-label">Failed</span>
          </div>
        </div>
      )}

      <div className="admin-controls">
        <button onClick={handleGenerate} disabled={generating}>
          {generating ? 'Generating...' : 'Generate Synthetic Ticket'}
        </button>
        <button onClick={refresh} className="btn-secondary">Refresh</button>
      </div>

      {error && <div className="error-msg">{error}</div>}

      <TicketTable
        tickets={tickets}
        onProcess={handleProcess}
        processingIds={processingIds}
      />
    </div>
  )
}
