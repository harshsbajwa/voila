// API Configuration for development and production

// Determine API base URL based on environment
const getApiBaseUrl = (): string => {
  // In development, the backend is likely running on port 8000
  if (import.meta.env.DEV) {
    return import.meta.env.VITE_API_URL || 'http://localhost:8000'
  }
  
  // In production, use the same origin or environment variable
  return import.meta.env.VITE_API_URL || window.location.origin
}

export const API_BASE_URL = getApiBaseUrl()

// API endpoints configuration
export const API_ENDPOINTS = {
  // Core data endpoints
  companies: '/api/v1/data/companies',
  completeMarketData: (ticker: string) => `/api/v1/data/complete/${ticker}`,
  bulkCompleteMarketData: '/api/v1/data/complete/bulk',
  search: '/api/v1/data/search',
  dataSummary: '/api/v1/data/stats/summary',
  
  // Market data endpoints
  ohlcv: (ticker: string) => `/api/v1/market-data/ohlcv/${ticker}`,
  latestPrice: (ticker: string) => `/api/v1/market-data/latest/${ticker}`,
  bulkMarketData: '/api/v1/market-data/bulk-with-location',
  marketOverview: '/api/v1/market-data/market-overview',
  timeSeriesAnalysis: (ticker: string) => `/api/v1/market-data/time-series-analysis/${ticker}`,
  
  // Spatial/geospatial endpoints
  withinCircle: '/api/v1/spatial/within-circle',
  withinPolygon: '/api/v1/spatial/within-polygon',
  byState: (state: string) => `/api/v1/spatial/by-state/${state}`,
  nearbyTicker: '/api/v1/spatial/nearby-ticker',
  regionalStats: '/api/v1/spatial/regional-stats',
  
  // System endpoints
  health: '/health',
  cacheStats: '/cache/stats',
  clearCache: '/cache/clear'
} as const

// Request configuration
export const API_CONFIG = {
  timeout: 30000, // 30 seconds
  retries: 3,
  retryDelay: 1000, // 1 second
  
  // Default headers
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  },
  
  // Request rate limiting (client-side)
  rateLimits: {
    search: { requests: 60, window: 60000 }, // 60 requests per minute
    spatial: { requests: 40, window: 60000 }, // 40 requests per minute
    marketData: { requests: 120, window: 60000 }, // 120 requests per minute
    general: { requests: 200, window: 60000 } // 200 requests per minute
  }
} as const

// Error codes and messages
export const API_ERRORS = {
  NETWORK_ERROR: 'Network connection failed',
  TIMEOUT_ERROR: 'Request timed out',
  RATE_LIMITED: 'Rate limit exceeded, please try again later',
  SERVER_ERROR: 'Server error occurred',
  NOT_FOUND: 'Resource not found',
  VALIDATION_ERROR: 'Invalid request data'
} as const

// WebSocket configuration
export const WS_CONFIG = {
  url: API_BASE_URL.replace('http', 'ws') + '/ws',
  reconnectDelay: 3000,
  maxReconnectAttempts: 10,
  heartbeatInterval: 30000
} as const

// Cache configuration
export const CACHE_CONFIG = {
  defaultTTL: 5 * 60 * 1000, // 5 minutes
  longTTL: 30 * 60 * 1000, // 30 minutes
  shortTTL: 60 * 1000, // 1 minute
  
  // Cache keys
  keys: {
    companies: 'companies',
    marketOverview: 'market-overview',
    completeMarketData: 'complete-market-data',
    chartData: 'chart-data',
    spatialQuery: 'spatial-query',
    search: 'search'
  }
} as const