import type { CompletedMarketRecord, RegionalStats } from './api'

// Redux State Types

export interface CompaniesState {
  entities: Record<string, CompletedMarketRecord>
  visibleIds: string[]
  selectedIds: string[]
  hoveredId: string | null
  hoveredCompany: CompletedMarketRecord | null
  filters: CompanyFilters
  loading: boolean
  error: string | null
  lastUpdated: number | null
}

export interface CompanyFilters {
  sectors?: string[]
  states?: string[]
  priceRange?: [number, number]
  volumeRange?: [number, number]
  searchQuery?: string
  hasLocation?: boolean
}

export interface MapState {
  camera: {
    position: { x: number; y: number; z: number }
    zoom: number
    target: { x: number; y: number; z: number }
  }
  viewport: {
    bounds: {
      north: number
      south: number
      east: number
      west: number
    }
    center: { lat: number; lng: number }
    zoomLevel: number
  }
  selectionMode: SelectionMode
  selectionGeometry: SelectionGeometry | null
  lodLevel: number
  performanceMode: 'high' | 'medium' | 'low'
}

export interface SelectionState {
  selectedCompanies: Set<string>
  selectionStats: RegionalStats | null
  selectionHistory: SelectionHistoryEntry[]
  isSelecting: boolean
  selectionMode: SelectionMode
}

export interface SelectionHistoryEntry {
  id: string
  timestamp: number
  companyIds: string[]
  stats: RegionalStats
  mode: SelectionMode
  geometry: SelectionGeometry
}

export interface ChartsState {
  activeChart: ActiveChart | null
  chartHistory: ChartHistoryEntry[]
  chartSettings: ChartSettings
  indicators: TechnicalIndicator[]
}

export interface ActiveChart {
  ticker: string
  timeframe: string
  chartType: 'candlestick' | 'line' | 'area'
  data: any[]
  loading: boolean
  error: string | null
}

export interface ChartHistoryEntry {
  ticker: string
  timestamp: number
  timeframe: string
}

export interface ChartSettings {
  theme: 'dark' | 'light'
  showVolume: boolean
  showGrid: boolean
  candleWidth: number
  colors: {
    bullish: string
    bearish: string
    volume: string
    grid: string
  }
}

export interface TechnicalIndicator {
  id: string
  type: 'sma' | 'ema' | 'rsi' | 'macd' | 'bollinger'
  parameters: Record<string, any>
  enabled: boolean
  color: string
}

export interface RealtimeState {
  connected: boolean
  latency: number
  lastUpdate: number | null
  priceUpdates: Record<string, PriceUpdateEntry>
  alerts: MarketAlert[]
  subscriptions: Set<string>
}

export interface PriceUpdateEntry {
  ticker: string
  price: number
  change: number
  changePercent: number
  volume: number
  timestamp: number
}

export interface MarketAlert {
  id: string
  severity: 'info' | 'warning' | 'critical'
  message: string
  timestamp: number
  read: boolean
  affectedTickers?: string[]
}

// UI State Types
export interface UIState {
  theme: 'dark' | 'light'
  sidebarOpen: boolean
  chartModalOpen: boolean
  settingsModalOpen: boolean
  loadingStates: Record<string, boolean>
  notifications: Notification[]
  performanceStats: PerformanceStats
}

export interface Notification {
  id: string
  type: 'success' | 'error' | 'warning' | 'info'
  title: string
  message: string
  timestamp: number
  duration?: number
  actions?: NotificationAction[]
}

export interface NotificationAction {
  label: string
  action: () => void
}

export interface PerformanceStats {
  fps: number
  pointCount: number
  drawCalls: number
  memoryUsage: number
  renderTime: number
  lastUpdated: number
}

// Selection and Geometry Types
export type SelectionMode = 'pan' | 'rect' | 'circle' | 'polygon' | 'lasso' | 'state'

export interface SelectionGeometry {
  type: SelectionMode
  bounds?: BoundingBox
  center?: Point
  radius?: number
  points?: Point[]
  state?: string
  start?: Point
  current?: Point
}

export interface BoundingBox {
  minX: number
  maxX: number
  minY: number
  maxY: number
}

export interface Point {
  x: number
  y: number
}

// Combined Root State
export interface RootState {
  companies: CompaniesState
  map: MapState
  selection: SelectionState
  charts: ChartsState
  realtime: RealtimeState
  ui: UIState
}