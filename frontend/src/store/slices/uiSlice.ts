import { createSlice, PayloadAction } from '@reduxjs/toolkit'
import type { UIState, Notification, PerformanceStats } from '@/types/state'

const initialState: UIState = {
  theme: 'dark',
  sidebarOpen: true,
  chartModalOpen: false,
  settingsModalOpen: false,
  loadingStates: {},
  notifications: [],
  performanceStats: {
    fps: 0,
    pointCount: 0,
    drawCalls: 0,
    memoryUsage: 0,
    renderTime: 0,
    lastUpdated: Date.now()
  }
}

let lastFrameTime = Date.now()

export const uiSlice = createSlice({
  name: 'ui',
  initialState,
  reducers: {
    // Theme
    setTheme: (state, action: PayloadAction<'dark' | 'light'>) => {
      state.theme = action.payload
    },

    toggleTheme: (state) => {
      state.theme = state.theme === 'dark' ? 'light' : 'dark'
    },

    // Sidebar
    setSidebarOpen: (state, action: PayloadAction<boolean>) => {
      state.sidebarOpen = action.payload
    },

    toggleSidebar: (state) => {
      state.sidebarOpen = !state.sidebarOpen
    },

    // Chart Modal
    openChartModal: (state) => {
      state.chartModalOpen = true
    },

    closeChartModal: (state) => {
      state.chartModalOpen = false
    },

    // Settings Modal
    openSettingsModal: (state) => {
      state.settingsModalOpen = true
    },

    closeSettingsModal: (state) => {
      state.settingsModalOpen = false
    },

    // Loading states
    setLoadingState: (state, action: PayloadAction<{ key: string; loading: boolean }>) => {
      const { key, loading } = action.payload
      if (loading) {
        state.loadingStates[key] = true
      } else {
        delete state.loadingStates[key]
      }
    },

    clearLoadingStates: (state) => {
      state.loadingStates = {}
    },

    // Notifications
    addNotification: (state, action: PayloadAction<Omit<Notification, 'id' | 'timestamp'>>) => {
      const notification: Notification = {
        ...action.payload,
        id: `notification_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        timestamp: Date.now()
      }
      
      state.notifications.unshift(notification)
      
      if (state.notifications.length > 10) {
        state.notifications = state.notifications.slice(0, 10)
      }
    },

    removeNotification: (state, action: PayloadAction<string>) => {
      state.notifications = state.notifications.filter(
        notification => notification.id !== action.payload
      )
    },

    clearAllNotifications: (state) => {
      state.notifications = []
    },

    // Performance stats
    updatePerformanceStats: (state, action: PayloadAction<Partial<PerformanceStats>>) => {
      const now = Date.now()
      const delta = (now - lastFrameTime) / 1000
      lastFrameTime = now
      const fps = Math.round(1 / delta)

      state.performanceStats = {
        ...state.performanceStats,
        ...action.payload,
        fps,
        lastUpdated: now,
      }
    },

    // UI state presets
    applyUIPreset: (state, action: PayloadAction<'default' | 'minimal' | 'presentation'>) => {
      const preset = action.payload
      
      switch (preset) {
        case 'default':
          state.sidebarOpen = true
          break
        case 'minimal':
          state.sidebarOpen = false
          state.chartModalOpen = false
          state.settingsModalOpen = false
          break
        case 'presentation':
          state.sidebarOpen = false
          state.chartModalOpen = false
          state.settingsModalOpen = false
          break
      }
    },
  }
})