import React from 'react'
import { useSelector } from 'react-redux'
import type { RootState } from '@/types/state'

export const PerformanceMonitor: React.FC = () => {
  const performanceStats = useSelector((state: RootState) => state.ui.performanceStats)

  return (
    <div className="perf-monitor">
      FPS: <span className="perf-stat">{performanceStats.fps}</span> | 
      Points: <span className="perf-stat">{performanceStats.pointCount}</span> | 
      Draws: <span className="perf-stat">{performanceStats.drawCalls}</span>
      {performanceStats.memoryUsage > 0 && (
        <>
          {' | '}Mem: <span className="perf-stat">
            {(performanceStats.memoryUsage / 1024 / 1024).toFixed(1)}MB
          </span>
        </>
      )}
    </div>
  )
}