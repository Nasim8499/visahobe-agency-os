import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const API_TARGET = process.env.VITE_API_TARGET ?? 'http://127.0.0.1:8787';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    allowedHosts: true,
    port: 5173,
    fs: { allow: ['..', '../..'] },
    proxy: {
      // changeOrigin stays false on purpose: the API treats "same host as the
      // request" as a trusted origin, so forwarding the original Host header
      // keeps tunnel/preview hosts working without an allow-list entry.
      '/api': { target: API_TARGET, changeOrigin: false, ws: true },
      '/health': { target: API_TARGET, changeOrigin: false },
    },
  },
  preview: {
    host: '0.0.0.0',
    allowedHosts: true,
    port: 4173,
    proxy: {
      '/api': { target: API_TARGET, changeOrigin: false, ws: true },
      '/health': { target: API_TARGET, changeOrigin: false },
    },
  },
  build: { outDir: 'dist', sourcemap: true, target: 'es2022' },
});
