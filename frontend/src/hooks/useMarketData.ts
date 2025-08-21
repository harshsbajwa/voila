import { useEffect } from 'react'
import { useQuery, useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useDispatch, useSelector } from 'react-redux'
import { queryFunctions, prefetchStrategies } from '@/api/queryClient'
import type { RootState } from '@/types/state'
import type { 
  CompletedMarketRecord, 
  MarketOverviewResponse,
  CircleQuery,
  PolygonQuery,
} from '@/types/api'

// Hook for companies data with filtering and caching
export const useCompanies = (filters?: any) => {
  const dispatch = useDispatch()
  const { filters: storeFilters } = useSelector((state: RootState) => state.companies)
  
  const finalFilters = filters || storeFilters

  const query = useQuery<CompletedMarketRecord[], Error>({
    queryKey: ['companies', finalFilters],
    queryFn: queryFunctions.companies,
    staleTime: 5 * 60 * 1000, // 5 minutes
  })

  // Update Redux store when data changes
  useEffect(() => {
    if (query.data) {
      dispatch({ type: 'companies/setCompanies', payload: query.data })
    }
  }, [query.data, dispatch])

  // Handle errors
  useEffect(() => {
    if (query.error) {
      dispatch({ 
        type: 'companies/setError', 
        payload: query.error.message
      })
    }
  }, [query.error, dispatch])

  return query
}

// Hook for infinite loading of companies
export const useInfiniteCompanies = (baseFilters?: any) => {
  return useInfiniteQuery({
    queryKey: ['companies-infinite', baseFilters],
    queryFn: async () => {
      return queryFunctions.companies()
    },
    getNextPageParam: (lastPage, allPages) => {
      if (lastPage.length < 1000) return undefined
      return allPages.length * 1000
    },
    staleTime: 10 * 60 * 1000, // 10 minutes for paginated data
    initialPageParam: 0
  })
}

// Hook for single company data
export const useCompany = (ticker: string) => {
  return useQuery({
    queryKey: ['complete-market-data', ticker],
    queryFn: queryFunctions.completeMarketData,
    enabled: !!ticker,
    staleTime: 15 * 60 * 1000 // 15 minutes
  })
}

// Hook for chart data with real-time updates
export const useChartData = (ticker: string, timeframe = '1D', limit = 90) => {
  const dispatch = useDispatch()
  const { priceUpdates } = useSelector((state: RootState) => state.realtime)

  const query = useQuery({
    queryKey: ['chart-data', ticker, timeframe, limit],
    queryFn: queryFunctions.chartData,
    enabled: !!ticker,
    staleTime: 60 * 1000, // 1 minute for real-time data
    refetchInterval: 30 * 1000, // Refetch every 30 seconds
  })

  // Update Redux store when data changes
  useEffect(() => {
    if (query.data) {
      dispatch({ type: 'charts/setChartData', payload: query.data })
    }
  }, [query.data, dispatch])

  // Apply real-time updates to chart data
  const updatedData = query.data?.map(dataPoint => {
    const update = priceUpdates[ticker]
    if (update && dataPoint.Date === new Date().toISOString().split('T')[0]) {
      return {
        ...dataPoint,
        Close: update.price,
        Volume: update.volume
      }
    }
    return dataPoint
  })

  return {
    ...query,
    data: updatedData || query.data
  }
}

// Hook for market overview with automatic updates
export const useMarketOverview = () => {
  return useQuery<MarketOverviewResponse, Error>({
    queryKey: ['market-overview'],
    queryFn: queryFunctions.marketOverview,
    staleTime: 2 * 60 * 1000, // 2 minutes
    refetchInterval: 60 * 1000, // Refetch every minute
  })
}

// Hook for spatial queries
export const useSpatialQuery = (queryType: string, params: any) => {
  const query = useQuery({
    queryKey: ['spatial-query', queryType, params],
    queryFn: queryFunctions.spatialQuery,
    enabled: !!queryType && !!params,
    staleTime: 30 * 1000, // 30 seconds for spatial data
  })

  // Log errors
  useEffect(() => {
    if (query.error) {
    }
  }, [query.error])

  return query
}

// Hook for circle selection queries
export const useCircleSelection = (query: CircleQuery | null) => {
  return useSpatialQuery('within-circle', query)
}

// Hook for polygon selection queries  
export const usePolygonSelection = (query: PolygonQuery | null) => {
  return useSpatialQuery('within-polygon', query)
}

// Mutation hooks for data modifications
export const useUpdateCompanySelection = () => {
  const queryClient = useQueryClient()
  const dispatch = useDispatch()

  return useMutation({
    mutationFn: async (selectedTickers: string[]) => {
      // Optimistically update selection
      dispatch({ 
        type: 'selection/setSelectedCompanies', 
        payload: selectedTickers 
      })

      // Prefetch chart data for selected companies
      prefetchStrategies.prefetchChartsForSelection(selectedTickers)

      return selectedTickers
    },
    onSuccess: () => {
      // Invalidate related queries
      queryClient.invalidateQueries({ queryKey: ['selection-stats'] })
    }
  })
}

// Hook for bulk data operations
export const useBulkMarketData = () => {
  return useMutation({
    mutationFn: queryFunctions.bulkCompleteMarketData,
    onError: () => {
    }
  })
}

// Hook for prefetching strategies
export const usePrefetchStrategies = () => {
  const queryClient = useQueryClient()

  return {
    prefetchCompanies: () => prefetchStrategies.prefetchCompanies(),
    prefetchMarketOverview: () => prefetchStrategies.prefetchMarketOverview(),
    prefetchChartsForSelection: (tickers: string[]) => 
      prefetchStrategies.prefetchChartsForSelection(tickers),
    
    // Smart prefetching based on user behavior
    prefetchNearbyData: async (centerLat: number, centerLng: number) => {
      await queryClient.prefetchQuery({
        queryKey: ['spatial-query', 'nearby', { latitude: centerLat, longitude: centerLng }],
        queryFn: queryFunctions.spatialQuery,
        staleTime: 60 * 1000
      })
    }
  }
}

// Hook for managing data subscriptions
export const useDataSubscriptions = () => {
  const dispatch = useDispatch()
  const { selectedCompanies } = useSelector((state: RootState) => state.selection)
  const { subscriptions } = useSelector((state: RootState) => state.realtime)

  const subscribe = (tickers: string[]) => {
    tickers.forEach(ticker => {
      if (!subscriptions.has(ticker)) {
        dispatch({ type: 'realtime/addSubscription', payload: ticker })
      }
    })
  }

  const unsubscribe = (tickers: string[]) => {
    tickers.forEach(ticker => {
      dispatch({ type: 'realtime/removeSubscription', payload: ticker })
    })
  }

  const subscribeToSelection = () => {
    const tickerArray = Array.from(selectedCompanies)
    subscribe(tickerArray)
  }

  const unsubscribeFromAll = () => {
    dispatch({ type: 'realtime/clearSubscriptions' })
  }

  return {
    subscribe,
    unsubscribe,
    subscribeToSelection,
    unsubscribeFromAll,
    subscriptions: Array.from(subscriptions)
  }
}