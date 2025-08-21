import type { Middleware, AnyAction, MiddlewareAPI } from '@reduxjs/toolkit'
import type { RootState } from '@/types/state'
import type { WebSocketMessage } from '@/types/api'

let ws: WebSocket | null = null
let reconnectAttempts = 0
const maxReconnectAttempts = 5
const reconnectDelay = 1000

export const websocketMiddleware: Middleware<{}, RootState> = (store) => {
  const connect = () => {
    try {
      // Use the API base URL for WebSocket connections
      const wsUrl = import.meta.env.VITE_API_URL 
        ? import.meta.env.VITE_API_URL.replace('http', 'ws') + '/ws/market-feed'
        : 'ws://localhost:8000/ws/market-feed'
      
      ws = new WebSocket(wsUrl)
      
      ws.onopen = () => {
        reconnectAttempts = 0
        store.dispatch({ type: 'realtime/setConnectionStatus', payload: true })
        
        // Subscribe to currently selected companies
        const state = store.getState()
        const selectedCompanies = Array.from(state.selection.selectedCompanies)
        
        if (selectedCompanies.length > 0) {
          ws?.send(JSON.stringify({
            type: 'subscribe',
            tickers: selectedCompanies
          }))
        }
      }
      
      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data)
          handleWebSocketMessage(message, store)
        } catch (error) {
        }
      }
      
      ws.onclose = () => {
        store.dispatch({ type: 'realtime/setConnectionStatus', payload: false })
        ws = null
        
        // Only attempt reconnection if we previously had a successful connection
        if (reconnectAttempts > 0 && reconnectAttempts < maxReconnectAttempts) {
          reconnectAttempts++
          
          setTimeout(() => {
            connect()
          }, reconnectDelay * reconnectAttempts)
        } else if (reconnectAttempts >= maxReconnectAttempts) {
          store.dispatch({
            type: 'realtime/addAlert',
            payload: {
              severity: 'info' as const,
              message: 'Real-time data unavailable. Using cached data.',
              timestamp: Date.now()
            }
          })
        }
      }
      
      ws.onerror = () => {
        // Don't show alerts for initial connection failures
        if (reconnectAttempts > 0) {
          store.dispatch({
            type: 'realtime/addAlert',
            payload: {
              severity: 'warning' as const,
              message: 'Real-time data connection lost. Operating in cached mode.',
              timestamp: Date.now()
            }
          })
        }
      }
    } catch (error) {
    }
  }

  const disconnect = () => {
    if (ws) {
      ws.close()
      ws = null
    }
  }

  return (next) => (action: unknown) => {
    switch ((action as AnyAction).type) {
      case 'realtime/connect':
        if (!ws || ws.readyState === WebSocket.CLOSED) {
          connect()
        }
        break
        
      case 'realtime/disconnect':
        disconnect()
        break
        
      case 'selection/setSelectedCompanies':
      case 'selection/addToSelection':
        // Subscribe to new selections
        if (ws && ws.readyState === WebSocket.OPEN) {
          const tickers = Array.isArray((action as AnyAction).payload) ? (action as AnyAction).payload : [(action as AnyAction).payload]
          ws.send(JSON.stringify({
            type: 'subscribe',
            tickers
          }))
        }
        break
        
      case 'selection/removeFromSelection':
      case 'selection/clearSelection':
        // Unsubscribe from removed selections
        if (ws && ws.readyState === WebSocket.OPEN) {
          const tickers = Array.isArray((action as AnyAction).payload) ? (action as AnyAction).payload : [(action as AnyAction).payload]
          ws.send(JSON.stringify({
            type: 'unsubscribe',
            tickers
          }))
        }
        break
    }

    return next(action)
  }
}

function handleWebSocketMessage(message: WebSocketMessage, store: MiddlewareAPI<any, RootState>) {
  const startTime = performance.now()
  
  switch (message.type) {
    case 'price_update':
      store.dispatch({
        type: 'realtime/addPriceUpdate',
        payload: {
          ticker: (message.data as any).ticker,
          price: (message.data as any).price,
          change: (message.data as any).change,
          changePercent: (message.data as any).change_percent,
          volume: (message.data as any).volume,
          timestamp: message.timestamp
        }
      })
      break
      
    case 'market_alert':
      store.dispatch({
        type: 'realtime/addAlert',
        payload: {
          severity: (message.data as any).severity,
          message: (message.data as any).message,
          timestamp: message.timestamp,
          affectedTickers: (message.data as any).affected_tickers
        }
      })
      break
      
    case 'system_status':
      store.dispatch({
        type: 'realtime/updateLatency',
        payload: (message.data as any).latency || 0
      })
      break
      
    case 'batch_update':
      if (message.data && Array.isArray(message.data)) {
        store.dispatch({
          type: 'realtime/batchPriceUpdates',
          payload: message.data.map((update: any) => ({
            ticker: update.ticker,
            price: update.price,
            change: update.change,
            changePercent: update.change_percent,
            volume: update.volume,
            timestamp: update.timestamp
          }))
        })
      }
      break
      
    default:
  }
  
  // Record processing time for performance monitoring
  const processingTime = performance.now() - startTime
  store.dispatch({
    type: 'realtime/recordMetrics',
    payload: {
      messageRate: 1, // TODO: calculate over time
      processingTime,
      queueLength: 0 // TODO: track
    }
  })
}