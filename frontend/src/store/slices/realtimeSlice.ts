import { createSlice, PayloadAction } from '@reduxjs/toolkit'
import type { RealtimeState, PriceUpdateEntry, MarketAlert } from '@/types/state'

const initialState: RealtimeState = {
  connected: false,
  latency: 0,
  lastUpdate: null,
  priceUpdates: {},
  alerts: [],
  subscriptions: new Set()
}

export const realtimeSlice = createSlice({
  name: 'realtime',
  initialState,
  reducers: {
    setConnectionStatus: (state, action: PayloadAction<boolean>) => {
      state.connected = action.payload
      
      if (!action.payload) {
        state.subscriptions.clear()
      }
    },

    updateLatency: (state, action: PayloadAction<number>) => {
      state.latency = action.payload
    },

    setLastUpdate: (state, action: PayloadAction<number>) => {
      state.lastUpdate = action.payload
    },

    addPriceUpdate: (state, action: PayloadAction<PriceUpdateEntry>) => {
      const update = action.payload
      state.priceUpdates[update.ticker] = update
      state.lastUpdate = update.timestamp
    },

    batchPriceUpdates: (state, action: PayloadAction<PriceUpdateEntry[]>) => {
      action.payload.forEach(update => {
        state.priceUpdates[update.ticker] = update
      })
      
      const latestTimestamp = Math.max(
        ...action.payload.map(update => update.timestamp)
      )
      state.lastUpdate = latestTimestamp
    },

    clearPriceUpdates: (state) => {
      state.priceUpdates = {}
    },

    addAlert: (state, action: PayloadAction<Omit<MarketAlert, 'id' | 'read'>>) => {
      const alert: MarketAlert = {
        ...action.payload,
        id: `alert_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        read: false
      }
      
      state.alerts.unshift(alert)
      
      if (state.alerts.length > 100) {
        state.alerts = state.alerts.slice(0, 100)
      }
    },

    markAlertAsRead: (state, action: PayloadAction<string>) => {
      const alert = state.alerts.find(a => a.id === action.payload)
      if (alert) {
        alert.read = true
      }
    },

    markAllAlertsAsRead: (state) => {
      state.alerts.forEach(alert => {
        alert.read = true
      })
    },

    removeAlert: (state, action: PayloadAction<string>) => {
      state.alerts = state.alerts.filter(alert => alert.id !== action.payload)
    },

    clearAlerts: (state) => {
      state.alerts = []
    },

    addSubscription: (state, action: PayloadAction<string>) => {
      state.subscriptions.add(action.payload)
    },

    removeSubscription: (state, action: PayloadAction<string>) => {
      state.subscriptions.delete(action.payload)
    },

    batchAddSubscriptions: (state, action: PayloadAction<string[]>) => {
      action.payload.forEach(ticker => state.subscriptions.add(ticker))
    },

    batchRemoveSubscriptions: (state, action: PayloadAction<string[]>) => {
      action.payload.forEach(ticker => state.subscriptions.delete(ticker))
    },

    clearSubscriptions: (state) => {
      state.subscriptions.clear()
    },

    updateConnectionQuality: (state, action: PayloadAction<{
      latency: number
      packetsLost?: number
      reconnectAttempts?: number
    }>) => {
      const { latency } = action.payload
      state.latency = latency
      
      if (latency > 5000) {
        const alert: MarketAlert = {
          id: `connection_${Date.now()}`,
          severity: 'warning',
          message: `High latency detected: ${latency}ms. Real-time data may be delayed.`,
          timestamp: Date.now(),
          read: false
        }
        
        state.alerts.unshift(alert)
      }
    },

    updateSystemStatus: (state, action: PayloadAction<{
      status: 'healthy' | 'degraded' | 'offline'
      message?: string
    }>) => {
      const { status, message } = action.payload
      
      if (status === 'offline') {
        state.connected = false
        state.subscriptions.clear()
      }
      
      if (message) {
        const severity = status === 'offline' ? 'critical' : 
                        status === 'degraded' ? 'warning' : 'info'
        
        const alert: MarketAlert = {
          id: `system_${Date.now()}`,
          severity,
          message,
          timestamp: Date.now(),
          read: false
        }
        
        state.alerts.unshift(alert)
      }
    },

    recordMetrics: (state) => {
      state.lastUpdate = Date.now()
    }
  }
})

export const selectUnreadAlerts = (state: { realtime: RealtimeState }) =>
  state.realtime.alerts.filter(alert => !alert.read)

export const selectCriticalAlerts = (state: { realtime: RealtimeState }) =>
  state.realtime.alerts.filter(alert => alert.severity === 'critical' && !alert.read)

export const selectSubscriptionsArray = (state: { realtime: RealtimeState }) =>
  Array.from(state.realtime.subscriptions)

export const selectConnectionHealth = (state: { realtime: RealtimeState }) => {
  const { connected, latency, lastUpdate } = state.realtime
  
  if (!connected) return 'disconnected'
  
  const timeSinceUpdate = Date.now() - (lastUpdate || 0)
  
  if (latency > 5000 || timeSinceUpdate > 30000) return 'poor'
  if (latency > 2000 || timeSinceUpdate > 15000) return 'fair'
  return 'good'
}