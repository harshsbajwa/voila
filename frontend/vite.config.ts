import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [
    react({
      jsxRuntime: 'automatic',
    })
  ],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    }
  },
  server: {
    port: 3001,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true
      },
      '/ws': {
        target: 'ws://localhost:8000',
        ws: true
      }
    }
  },
  build: {
    target: 'es2020',
    sourcemap: false,
    minify: 'esbuild',
    rollupOptions: {
      output: {
        manualChunks: {
          three: ['three'],
          d3: ['d3', 'topojson-client'],
          react: ['react', 'react-dom', 'react-redux'],
          redux: ['@reduxjs/toolkit'],
          query: ['@tanstack/react-query'],
          vendor: ['comlink', 'idb']
        }
      }
    }
  },
  worker: {
    format: 'es'
  },
  optimizeDeps: {
    include: [
      'react',
      'react-dom',
      '@reduxjs/toolkit',
      'react-redux',
      '@tanstack/react-query',
      'three',
      'd3',
      'topojson-client',
      'comlink',
      'idb'
    ]
  }
})