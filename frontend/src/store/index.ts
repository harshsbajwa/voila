import { configureStore } from '@reduxjs/toolkit'
import { companiesSlice } from './slices/companiesSlice'
import { mapSlice } from './slices/mapSlice'
import { selectionSlice } from './slices/selectionSlice'
import { chartsSlice } from './slices/chartsSlice'
import { realtimeSlice } from './slices/realtimeSlice'
import { uiSlice } from './slices/uiSlice'
import { websocketMiddleware } from './middleware/websocketMiddleware'
import { cacheMiddleware } from './middleware/cacheMiddleware'
import { analyticsMiddleware } from './middleware/analyticsMiddleware'

export const store = configureStore({
  reducer: {
    companies: companiesSlice.reducer,
    map: mapSlice.reducer,
    selection: selectionSlice.reducer,
    charts: chartsSlice.reducer,
    realtime: realtimeSlice.reducer,
    ui: uiSlice.reducer
  },
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: {
        // Ignore these paths in the state, as they contain non-serializable data (Set)
        ignoredPaths: ['selection.selectedCompanies', 'realtime.subscriptions'],
      }
    }).concat(
      websocketMiddleware,
      cacheMiddleware,
      analyticsMiddleware
    ),
  devTools: process.env.NODE_ENV !== 'production'
})

export type RootState = ReturnType<typeof store.getState>
export type AppDispatch = typeof store.dispatch

export const {
  setCompanies,
  updateCompany,
  setVisibleCompanies,
  setHoveredCompany,
  setFilters,
  setLoading: setCompaniesLoading,
  setError: setCompaniesError
} = companiesSlice.actions

export const {
  updateCamera,
  setViewport,
  setSelectionMode,
  setLODLevel,
  setPerformanceMode
} = mapSlice.actions

export const {
  setSelectedCompanies,
  addToSelection,
  removeFromSelection,
  clearSelection,
  setSelectionStats,
  addToSelectionHistory
} = selectionSlice.actions

export const {
  setActiveChart,
  closeActiveChart,
  addChartToHistory,
  setChartSettings,
  addTechnicalIndicator,
  removeTechnicalIndicator,
  setChartLoading,
  setChartError
} = chartsSlice.actions

export const {
  setConnectionStatus,
  updateLatency,
  addPriceUpdate,
  addAlert,
  clearAlerts,
  addSubscription,
  removeSubscription
} = realtimeSlice.actions

export const {
  setTheme,
  toggleSidebar,
  openChartModal,
  closeChartModal,
  openSettingsModal,
  closeSettingsModal,
  setLoadingState,
  addNotification,
  removeNotification,
  updatePerformanceStats
} = uiSlice.actions