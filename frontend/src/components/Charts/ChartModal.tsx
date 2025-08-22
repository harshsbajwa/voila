import React, { Suspense } from 'react'
import { useSelector, useDispatch } from 'react-redux'
import type { RootState } from '@/types/state'
import { LoadingIndicator } from '../UI/LoadingIndicator'
import { closeChartModal, closeActiveChart } from '@/store'

const LazyChartRenderer = React.lazy(() => import('./ChartRenderer'))

export const ChartModal: React.FC = () => {
  const dispatch = useDispatch()
  const isOpen = useSelector((state: RootState) => state.ui.chartModalOpen)
  const activeChart = useSelector((state: RootState) => state.charts.activeChart)

  const handleClose = () => {
    dispatch(closeChartModal())
    dispatch(closeActiveChart())
  }

  if (!isOpen || !activeChart) return null

  return (
    <div className="chart-modal active mobile-responsive">
      <div className="chart-header">
        <div className="chart-title">
          {activeChart.ticker} - Price Chart
        </div>
        <button className="close-btn" onClick={handleClose}>
          ✕
        </button>
      </div>
      
      <div className="w-full h-full chart-container">
        <Suspense fallback={<LoadingIndicator size="md" message="Loading chart..." />}>
          <LazyChartRenderer ticker={activeChart.ticker} />
        </Suspense>
      </div>
    </div>
  )
}