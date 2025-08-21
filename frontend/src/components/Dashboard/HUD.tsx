import React from 'react'
import { useSelector } from 'react-redux'
import { useMarketOverview } from '@/hooks/useMarketData'
import { useSelectionStats } from '@/hooks/useSelection'
import type { RootState } from '@/types/state'

export const HUD: React.FC = () => {
  const { data: marketOverview, isLoading } = useMarketOverview()
  const selectionStats = useSelectionStats()
  const selectedCount = useSelector((state: RootState) => state.selection.selectedCompanies.size)
  const isSelectionActive = selectedCount > 0

  const displayStats = isSelectionActive ? selectionStats : marketOverview?.market_summary
  const displayTitle = isSelectionActive ? "Selection Overview" : "Market Overview"
  const topCompanies = isSelectionActive ? selectionStats?.top_companies : []

  return (
    <div className="hud">
      <h2>{displayTitle}</h2>
      
      <div className="stats-grid">
        <div className="stat-item">
          <div className="stat-label">Companies</div>
          <div className="stat-value">
            {isLoading && !displayStats ? '...' : (isSelectionActive ? selectedCount : (displayStats as { total_companies?: number; company_count?: number })?.total_companies || (displayStats as { total_companies?: number; company_count?: number })?.company_count) || 0}
          </div>
        </div>
        
        <div className="stat-item">
          <div className="stat-label">Avg Price</div>
          <div className="stat-value">
            ${displayStats?.avg_price?.toFixed(2) || '0.00'}
          </div>
        </div>
        
        <div className="stat-item col-span-2">
          <div className="stat-label">Total Volume</div>
          <div className="stat-value">
            {formatVolume(displayStats?.total_volume || 0)}
          </div>
        </div>
      </div>
      
      {isSelectionActive && topCompanies && topCompanies.length > 0 && (
        <div className="mt-4">
          <div className="text-sm text-gray-400 mb-2">Top Companies by Price</div>
          <div className="text-xs text-gray-500 space-y-1">
            {topCompanies.slice(0, 3).map(company => (
              <div key={company.ticker} className="flex justify-between">
                <span>{company.ticker}</span>
                <span>${company.latest_close.toFixed(2)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function formatVolume(volume: number): string {
  if (volume >= 1e9) return `${(volume / 1e9).toFixed(2)}B`
  if (volume >= 1e6) return `${(volume / 1e6).toFixed(2)}M`
  if (volume >= 1e3) return `${(volume / 1e3).toFixed(1)}K`
  return volume.toLocaleString()
}