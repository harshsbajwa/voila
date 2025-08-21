import React, { Suspense, useEffect } from 'react'
import { useDispatch } from 'react-redux'
import { ErrorBoundary } from 'react-error-boundary'
import { MapRenderer } from '../MapRenderer/MapRenderer'
import { HUD } from './HUD'
 
import { ChartModal } from '../Charts/ChartModal'
import { NotificationContainer } from '../UI/NotificationContainer'
import { LoadingIndicator } from '../UI/LoadingIndicator'
import { useCompanies, usePrefetchStrategies } from '@/hooks/useMarketData'
import { spatialWorkerManager } from '@/utils/spatialWorkerManager'

const ErrorFallback: React.FC<{ error: Error; resetErrorBoundary: () => void }> = ({ error, resetErrorBoundary }) => (
  <div className="flex items-center justify-center min-h-screen bg-gray-900 text-white">
    <div className="text-center">
      <h2 className="text-2xl font-bold mb-4">Something went wrong</h2>
      <p className="text-gray-400 mb-6">{error.message}</p>
      <button
        onClick={resetErrorBoundary}
        className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors"
      >
        Try again
      </button>
    </div>
  </div>
)

const LazySettingsModal = React.lazy(() => import('./SettingsModal'))
const LazySelectionHistory = React.lazy(() => import('./SelectionHistory'))

export const MarketDashboard: React.FC = () => {
  const dispatch = useDispatch()
  
  const { prefetchCompanies, prefetchMarketOverview } = usePrefetchStrategies()

  const {
    data: companiesData,
    isLoading: companiesLoading,
    error: companiesError
  } = useCompanies({ has_location: true, limit: 2000 })
  

  useEffect(() => {
    prefetchCompanies()
    prefetchMarketOverview()

    return () => {
      spatialWorkerManager.dispose()
    }
  }, [prefetchCompanies, prefetchMarketOverview])

  useEffect(() => {
    if (companiesData && companiesData.length > 0) {
    }
  }, [companiesData])

  useEffect(() => {
    if (companiesError) {
      dispatch({
        type: 'ui/addNotification',
        payload: {
          type: 'error' as const,
          title: 'Data Loading Failed',
          message: 'Failed to load company data. Please check your connection.',
          duration: 8000
        }
      })
    }
  }, [companiesError, dispatch])

  if (companiesLoading && !companiesData) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-900">
        <LoadingIndicator
          size="lg"
          message="Loading market data and initializing 3D visualization..."
        />
      </div>
    )
  }

  return (
    <ErrorBoundary
      FallbackComponent={ErrorFallback}
      onError={(error, errorInfo) => {
        console.error('Dashboard error:', error, errorInfo)
      }}
    >
      <div className="relative w-screen h-screen overflow-hidden bg-gray-900">
        <MapRenderer className="absolute inset-0" />
        <HUD />
        <ChartModal />
        
        <Suspense fallback={<LoadingIndicator size="sm" />}>
          <LazySettingsModal />
          <LazySelectionHistory />
        </Suspense>

        <NotificationContainer />
        

        <div className="sr-only" aria-live="polite" aria-atomic="true">
          {companiesLoading && "Loading market data..."}
        </div>
      </div>
    </ErrorBoundary>
  )
}