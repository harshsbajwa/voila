import { useCallback, useMemo } from 'react'
import { useDispatch, useSelector } from 'react-redux'
import { useMutation } from '@tanstack/react-query'
import { spatialWorkerManager } from '@/utils/spatialWorkerManager'
import type { RootState } from '@/types/state'
import type { SelectionMode } from '@/types/state'
import type { RegionalStats, CompletedMarketRecord } from '@/types/api'

type GetProjectedCoordsFn = () => Array<{ ticker: string; x: number; y: number; z: number }>;
type Point2D = { x: number, y: number };

export const useSelection = () => {
  const dispatch = useDispatch()
  
  const selection = useSelector((state: RootState) => state.selection)
  const companies = useSelector((state: RootState) => state.companies)
  const { selectionMode } = useSelector((state: RootState) => state.map)

  const selectedCompanies = selection.selectedCompanies

  const selectedCompanyData = useMemo(() => {
    const data: CompletedMarketRecord[] = []
    selectedCompanies.forEach(ticker => {
        const company = companies.entities[ticker]
        if(company) data.push(company)
    })
    return data;
  }, [selectedCompanies, companies.entities])

  const setSelectionMode = useCallback((mode: SelectionMode) => {
    dispatch({ type: 'map/setSelectionMode', payload: mode })
  }, [dispatch])

  const performSpatialSelection = useMutation<any[], Error, { 
      type: 'rectangle' | 'circle' | 'polygon', 
      geometry: any,
      getProjectedCoords: GetProjectedCoordsFn
    }>({
    mutationFn: async ({ type, geometry, getProjectedCoords }) => {
      const allCoords = getProjectedCoords()
      if (!allCoords || allCoords.length === 0) return []

      switch (type) {
        case 'rectangle':
          return await spatialWorkerManager.selectInRectangle(geometry.min, geometry.max, allCoords)
        case 'circle':
          return await spatialWorkerManager.selectInCircle(geometry.center, geometry.radius, allCoords)
        case 'polygon':
          return await spatialWorkerManager.selectInPolygon(geometry.points, allCoords)
        default:
          throw new Error(`Unknown selection type: ${type}`)
      }
    },
    onSuccess: (result) => {
      const tickers = result.map((company: any) => company.ticker)
      dispatch({ type: 'selection/setSelectedCompanies', payload: tickers })
      
      dispatch({
        type: 'ui/addNotification',
        payload: {
          type: 'success' as const,
          title: 'Selection Updated',
          message: `Selected ${tickers.length} companies`,
          duration: 2000
        }
      })
    },
    onError: (error) => {
      dispatch({
        type: 'ui/addNotification',
        payload: {
          type: 'error' as const,
          title: 'Selection Failed',
          message: error.message,
          duration: 4000
        }
      })
    },
    onSettled: () => {
      dispatch({ type: 'selection/setIsSelecting', payload: false })
    }
  })

  const selectRectangle = useCallback((min: Point2D, max: Point2D, getProjectedCoords: GetProjectedCoordsFn) => {
    dispatch({ type: 'selection/setIsSelecting', payload: true })
    performSpatialSelection.mutate({ type: 'rectangle', geometry: { min, max }, getProjectedCoords })
  }, [performSpatialSelection, dispatch])

  const selectCircle = useCallback((center: Point2D, radius: number, getProjectedCoords: GetProjectedCoordsFn) => {
    dispatch({ type: 'selection/setIsSelecting', payload: true })
    performSpatialSelection.mutate({ type: 'circle', geometry: { center, radius }, getProjectedCoords })
  }, [performSpatialSelection, dispatch])

  const selectPolygon = useCallback((points: Point2D[], getProjectedCoords: GetProjectedCoordsFn) => {
    dispatch({ type: 'selection/setIsSelecting', payload: true })
    performSpatialSelection.mutate({ type: 'polygon', geometry: { points }, getProjectedCoords })
  }, [performSpatialSelection, dispatch])

  return {
    selectedCompanies,
    selectedCompanyData,
    isSelecting: selection.isSelecting,
    selectionMode,
    setSelectionMode,
    selectRectangle,
    selectCircle,
    selectPolygon,
    isSpatialSelectionLoading: performSpatialSelection.isPending,
  }
}

export const useSelectionStats = () => {
  const { selectedCompanies } = useSelector((state: RootState) => state.selection)
  const companies = useSelector((state: RootState) => state.companies.entities)

  return useMemo(() => {
    const selectedData: CompletedMarketRecord[] = []
    selectedCompanies.forEach(ticker => {
        const company = companies[ticker]
        if(company) selectedData.push(company)
    })
    return calculateSelectionStats(selectedData)
  }, [selectedCompanies, companies])
}

function calculateSelectionStats(companies: CompletedMarketRecord[]): RegionalStats {
  if (companies.length === 0) {
    return {
      company_count: 0,
      avg_price: 0,
      total_volume: 0,
      top_companies: [],
      region_description: 'Selection'
    }
  }

  let totalPrice = 0
  let totalVolume = 0
  const validCompanies = companies.filter(c => c && typeof c.latest_close === 'number')

  validCompanies.forEach(company => {
    totalPrice += company.latest_close
    totalVolume += company.latest_volume || 0
  })

  const avgPrice = validCompanies.length > 0 ? totalPrice / validCompanies.length : 0

  const topCompanies = [...validCompanies]
    .sort((a, b) => b.latest_close - a.latest_close)
    .slice(0, 5)
    .map(company => ({
      ticker: company.ticker,
      name: company.company_name,
      latest_close: company.latest_close,
      sector: 'N/A'
    }))

  return {
    company_count: companies.length,
    avg_price: avgPrice,
    total_volume: totalVolume,
    top_companies: topCompanies,
    region_description: 'Custom Selection'
  }
}

export const useSelectionHistory = () => {
  const { selectionHistory } = useSelector((state: RootState) => state.selection)
  const dispatch = useDispatch()

  const clearHistory = useCallback(() => {
    dispatch({ type: 'selection/clearHistory' })
  }, [dispatch])

  const restoreSelection = useCallback((historyId: string) => {
    const entry = selectionHistory.find(h => h.id === historyId)
    if (entry) {
      dispatch({ type: 'selection/setSelectedCompanies', payload: entry.companyIds })
    }
  }, [selectionHistory, dispatch])

  const removeFromHistory = useCallback((historyId: string) => {
    dispatch({ type: 'selection/removeFromHistory', payload: historyId })
  }, [dispatch])

  return {
    history: selectionHistory,
    clearHistory,
    restoreSelection,
    removeFromHistory
  }
}