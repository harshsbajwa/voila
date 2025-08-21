import { createSlice, PayloadAction } from '@reduxjs/toolkit'
import type { ChartsState, ChartSettings, TechnicalIndicator } from '@/types/state'

const initialChartSettings: ChartSettings = {
  theme: 'dark',
  showVolume: true,
  showGrid: true,
  candleWidth: 8,
  colors: {
    bullish: '#00ff88',
    bearish: '#ff0044',
    volume: '#4488ff',
    grid: '#333333'
  }
}

const initialState: ChartsState = {
  activeChart: null,
  chartHistory: [],
  chartSettings: initialChartSettings,
  indicators: []
}

export const chartsSlice = createSlice({
  name: 'charts',
  initialState,
  reducers: {
    setActiveChart: (state, action: PayloadAction<{
      ticker: string
      timeframe?: string
      chartType?: 'candlestick' | 'line' | 'area'
    }>) => {
      const { ticker, timeframe = '1D', chartType = 'candlestick' } = action.payload
      
      state.activeChart = {
        ticker,
        timeframe,
        chartType,
        data: [],
        loading: true,
        error: null
      }
    },

    setChartData: (state, action: PayloadAction<any[]>) => {
      if (state.activeChart) {
        state.activeChart.data = action.payload
        state.activeChart.loading = false
        state.activeChart.error = null
      }
    },

    setChartLoading: (state, action: PayloadAction<boolean>) => {
      if (state.activeChart) {
        state.activeChart.loading = action.payload
      }
    },

    setChartError: (state, action: PayloadAction<string | null>) => {
      if (state.activeChart) {
        state.activeChart.error = action.payload
        state.activeChart.loading = false
      }
    },

    closeActiveChart: (state) => {
      if (state.activeChart) {
        // Add to history before closing
        const historyEntry = {
          ticker: state.activeChart.ticker,
          timestamp: Date.now(),
          timeframe: state.activeChart.timeframe
        }
        
        state.chartHistory.unshift(historyEntry)
        
        // Keep only last 20 charts in history
        if (state.chartHistory.length > 20) {
          state.chartHistory = state.chartHistory.slice(0, 20)
        }
      }
      
      state.activeChart = null
    },

    addChartToHistory: (state, action: PayloadAction<{
      ticker: string
      timeframe: string
    }>) => {
      const { ticker, timeframe } = action.payload
      
      // Check if this chart is already in recent history
      const existingIndex = state.chartHistory.findIndex(
        entry => entry.ticker === ticker && entry.timeframe === timeframe
      )
      
      if (existingIndex !== -1) {
        // Move to top
        const entry = state.chartHistory[existingIndex]
        state.chartHistory.splice(existingIndex, 1)
        state.chartHistory.unshift({ ...entry, timestamp: Date.now() })
      } else {
        // Add new entry
        state.chartHistory.unshift({
          ticker,
          timeframe,
          timestamp: Date.now()
        })
      }
      
      // Keep only last 20
      if (state.chartHistory.length > 20) {
        state.chartHistory = state.chartHistory.slice(0, 20)
      }
    },

    clearChartHistory: (state) => {
      state.chartHistory = []
    },

    // Chart settings
    setChartSettings: (state, action: PayloadAction<Partial<ChartSettings>>) => {
      state.chartSettings = { ...state.chartSettings, ...action.payload }
    },

    updateChartColors: (state, action: PayloadAction<Partial<ChartSettings['colors']>>) => {
      state.chartSettings.colors = { 
        ...state.chartSettings.colors, 
        ...action.payload 
      }
    },

    // Technical indicators
    addTechnicalIndicator: (state, action: PayloadAction<Omit<TechnicalIndicator, 'id'>>) => {
      const indicator: TechnicalIndicator = {
        ...action.payload,
        id: `indicator_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
      }
      
      state.indicators.push(indicator)
    },

    removeTechnicalIndicator: (state, action: PayloadAction<string>) => {
      state.indicators = state.indicators.filter(
        indicator => indicator.id !== action.payload
      )
    },

    updateTechnicalIndicator: (state, action: PayloadAction<{
      id: string
      updates: Partial<TechnicalIndicator>
    }>) => {
      const { id, updates } = action.payload
      const index = state.indicators.findIndex(indicator => indicator.id === id)
      
      if (index !== -1) {
        state.indicators[index] = { ...state.indicators[index], ...updates }
      }
    },

    toggleIndicator: (state, action: PayloadAction<string>) => {
      const indicator = state.indicators.find(ind => ind.id === action.payload)
      if (indicator) {
        indicator.enabled = !indicator.enabled
      }
    },

    clearAllIndicators: (state) => {
      state.indicators = []
    },

    // Chart type and timeframe
    setChartType: (state, action: PayloadAction<'candlestick' | 'line' | 'area'>) => {
      if (state.activeChart) {
        state.activeChart.chartType = action.payload
      }
    },

    setTimeframe: (state, action: PayloadAction<string>) => {
      if (state.activeChart) {
        state.activeChart.timeframe = action.payload
        state.activeChart.loading = true
        state.activeChart.error = null
      }
    },

    // Preset configurations
    applyChartPreset: (state, action: PayloadAction<'default' | 'minimal' | 'advanced'>) => {
      const preset = action.payload
      
      switch (preset) {
        case 'default':
          state.chartSettings = initialChartSettings
          state.indicators = []
          break
        case 'minimal':
          state.chartSettings = {
            ...initialChartSettings,
            showVolume: false,
            showGrid: false
          }
          state.indicators = []
          break
        case 'advanced':
          state.chartSettings = initialChartSettings
          // common technical indicators
          state.indicators = [
            {
              id: 'sma_20',
              type: 'sma',
              parameters: { period: 20 },
              enabled: true,
              color: '#ff9800'
            },
            {
              id: 'rsi_14',
              type: 'rsi',
              parameters: { period: 14 },
              enabled: true,
              color: '#9c27b0'
            }
          ]
          break
      }
    }
  }
})