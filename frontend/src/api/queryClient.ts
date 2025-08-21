import { QueryClient } from '@tanstack/react-query'
import { getCachedCompanies, getCachedChartData } from '@/store/middleware/cacheMiddleware'
import { API_BASE_URL, API_ENDPOINTS } from '@/config/api'
import { HttpError, createHttpError } from '@/utils/errors'
import { debug } from '@/utils/debug'
import type { 
  CompletedMarketRecord, 
  OHLCVResponseRecord,
  MarketOverviewResponse
} from '@/types/api'

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000,
      gcTime: 30 * 60 * 1000,
      retry: (failureCount, error: unknown) => {
        if (error instanceof HttpError) {
          if (error.isClientError() && error.status !== 429) {
            return false
          }
          if (error.isRetryable()) {
            return failureCount < 3
          }
        }
        return failureCount < 3
      },
      retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 30000),
      refetchOnWindowFocus: true,
      refetchOnReconnect: true,
      refetchOnMount: true,
      networkMode: 'offlineFirst'
    },
    mutations: {
      retry: 1,
      networkMode: 'online'
    }
  }
})

export const queryFunctions = {
  companies: async (): Promise<CompletedMarketRecord[]> => {
    debug.log('🏢 Fetching companies...')
    try {
      const response = await fetch(`${API_BASE_URL}${API_ENDPOINTS.bulkCompleteMarketData}?limit=2000`)
      debug.log('🏢 Companies API response status:', response.status)
      if (!response.ok) {
        throw await createHttpError(response)
      }
      const companies: CompletedMarketRecord[] = await response.json()
      debug.log('🏢 Fetched', companies.length, 'companies from API')
      return companies
    } catch (error) {
      debug.error('🏢 Failed to fetch companies from API:', error)
      const cachedData = await getCachedCompanies()
      if (cachedData && cachedData.length > 0) {
        debug.log('🏢 Using cached companies from IndexedDB')
        return cachedData
      }
      throw error
    }
  },

  chartData: async ({ queryKey }: { queryKey: [string, string, string?, number?] }): Promise<OHLCVResponseRecord[]> => {
    const [, ticker, , limit] = queryKey
    try {
      const params = new URLSearchParams()
      if (limit) params.set('limit', limit.toString())
      const response = await fetch(`${API_BASE_URL}${API_ENDPOINTS.ohlcv(ticker)}?${params}`)
      if (!response.ok) throw await createHttpError(response)
      return await response.json()
    } catch (error) {
      const cachedData = await getCachedChartData(ticker)
      if (cachedData && cachedData.length > 0) return cachedData
      throw error
    }
  },

  marketOverview: async (): Promise<MarketOverviewResponse> => {
    const response = await fetch(`${API_BASE_URL}${API_ENDPOINTS.marketOverview}`)
    if (!response.ok) throw await createHttpError(response)
    return await response.json()
  },

  completeMarketData: async ({ queryKey }: { queryKey: [string, string] }): Promise<CompletedMarketRecord> => {
    const [, ticker] = queryKey
    const response = await fetch(`${API_BASE_URL}${API_ENDPOINTS.completeMarketData(ticker)}`)
    if (!response.ok) throw await createHttpError(response)
    return await response.json()
  },

  spatialQuery: async ({ queryKey }: { queryKey: [string, string, any] }): Promise<any> => {
    const [, queryType, params] = queryKey
    let endpoint = ''
    let requestBody = null
    
    switch (queryType) {
      case 'within-circle':
        endpoint = API_ENDPOINTS.withinCircle
        requestBody = params
        break
      case 'within-polygon':
        endpoint = API_ENDPOINTS.withinPolygon
        requestBody = params
        break
      default:
        throw new Error(`Unknown spatial query type: ${queryType}`)
    }

    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody)
    })
    if (!response.ok) throw await createHttpError(response)
    return await response.json()
  },

  bulkCompleteMarketData: async (tickers: string[]): Promise<CompletedMarketRecord[]> => {
    const response = await fetch(`${API_BASE_URL}${API_ENDPOINTS.bulkCompleteMarketData}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tickers })
    })
    if (!response.ok) throw await createHttpError(response)
    return await response.json()
  },
}

export const prefetchStrategies = {
  prefetchCompanies: () => {
    queryClient.prefetchQuery({
      queryKey: ['companies'],
      queryFn: queryFunctions.companies,
      staleTime: 10 * 60 * 1000
    })
  },
  prefetchMarketOverview: () => {
    queryClient.prefetchQuery({
      queryKey: ['market-overview'],
      queryFn: queryFunctions.marketOverview,
      staleTime: 5 * 60 * 1000
    })
  },
  prefetchChartsForSelection: (tickers: string[]) => {
    tickers.forEach(ticker => {
      queryClient.prefetchQuery({
        queryKey: ['chart-data', ticker, '1D', 90],
        queryFn: queryFunctions.chartData,
        staleTime: 2 * 60 * 1000
      })
    })
  },
}