import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://localhost:8000',
      '/health': 'http://localhost:8000',
      '/events': 'http://localhost:8000',
      '/stats': 'http://localhost:8000',
    },
  },
})
