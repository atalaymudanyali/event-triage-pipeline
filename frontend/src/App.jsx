import { Link, Route, Routes } from 'react-router-dom'
import './App.css'
import AdminView from './pages/AdminView'
import CustomerView from './pages/CustomerView'

export default function App() {
  return (
    <div className="app">
      <nav className="nav">
        <Link to="/" className="nav-brand">Triage Pipeline</Link>
        <div className="nav-links">
          <Link to="/">Customer</Link>
          <Link to="/admin">Admin</Link>
        </div>
      </nav>
      <main className="main">
        <Routes>
          <Route path="/" element={<CustomerView />} />
          <Route path="/admin" element={<AdminView />} />
        </Routes>
      </main>
    </div>
  )
}
