// FastAPI Response Types - Matching backend exactly

export interface CompanyRecord {
  ticker: string
  name: string
  address?: string
  latitude?: number
  longitude?: number
}

export interface OHLCVResponseRecord {
  Date: string
  Open: number
  High: number
  Low: number
  Close: number
  Volume: number
}

export interface CompletedMarketRecord {
  ticker: string
  name: string
  company_name: string
  address?: string
  latitude?: number
  longitude?: number
  latest_date?: string
  latest_open: number
  latest_high: number
  latest_low: number
  latest_close: number
  latest_volume: number
  price_change_24h?: number
  price_change_pct_24h?: number
  avg_volume_30d?: number
  volatility_30d?: number
}

export interface CompletedMarketRecordWithHistory extends CompletedMarketRecord {
  historical_data: OHLCVResponseRecord[]
}

export interface CompanyLocation {
  ticker: string
  name: string
  address?: string
  latitude: number
  longitude: number
  distance_km?: number
  latest_price?: number
  latest_volume?: number
  last_updated?: string
}

export interface RegionalStats {
  region_description: string;
  company_count: number
  avg_price?: number
  total_volume?: number
  top_companies?: {
    ticker: string;
    name: string;
    latest_close: number;
    sector: string;
  }[];
}

export interface SpatialQueryResponse {
  companies: CompanyLocation[]
  total_found: number
  query_params: Record<string, any>
  execution_time_ms: number
}

export interface RegionStatsResponse {
  stats: RegionalStats
  period: {
    start_date: string
    end_date: string
  }
  execution_time_ms: number
}

export interface MarketOverviewResponse {
  market_summary: {
    total_companies: number
    avg_price?: number
    total_volume: number
  }
  geographical_distribution: Array<{
    state: string
    company_count: number
    avg_price?: number
    total_volume: number
  }>
  timestamp: string
}

// API Request Types
export interface CircleQuery {
  latitude: number
  longitude: number
  radius_km: number
  limit?: number
  include_market_data?: boolean
}

export interface PolygonQuery {
  coordinates: Array<[number, number]>
  limit?: number
  include_market_data?: boolean
}

export interface BulkTickersRequest {
  tickers: string[]
  limit?: number
}

export interface TimeSeriesQuery {
  ticker: string
  start_date?: string
  end_date?: string
  limit?: number
}

export interface SearchQuery {
  q: string
  limit?: number
  offset?: number
  filters?: {
    sectors?: string[]
    states?: string[]
    has_location?: boolean
  }
}

// WebSocket Message Types
export interface WebSocketMessage {
  type: string
  data: any
  timestamp: number
}

export interface PriceUpdate {
  type: 'price_update'
  ticker: string
  price: number
  change: number
  change_percent: number
  volume: number
  timestamp: number
}

export interface MarketAlert {
  type: 'market_alert' 
  severity: 'info' | 'warning' | 'critical'
  message: string
  affected_tickers?: string[]
  timestamp: number
}

export interface SystemStatus {
  type: 'system_status'
  status: 'connected' | 'disconnected' | 'error'
  latency?: number
  timestamp: number
}