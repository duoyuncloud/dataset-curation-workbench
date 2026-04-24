import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        /** Large JSONL / slow first response; default proxy timeout is short and surfaces as "request timeout". */
        timeout: 120_000,
        proxyTimeout: 120_000,
      },
    },
  },
})
