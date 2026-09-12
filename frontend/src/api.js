const BASE = '/api/tickets'

async function request(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail || 'Request failed')
  }
  return res.json()
}

export const api = {
  createTicket: (data) => request(BASE, { method: 'POST', body: JSON.stringify(data) }),
  generateTicket: () => request(`${BASE}/generate`, { method: 'POST' }),
  listTickets: (status) => request(status ? `${BASE}?status=${status}` : BASE),
  getTicket: (id) => request(`${BASE}/${id}`),
  processTicket: (id) => request(`${BASE}/${id}/process`, { method: 'POST' }),
  getStats: () => request(`${BASE}/stats`),
}
