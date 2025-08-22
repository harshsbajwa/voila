import React from 'react'
import { useSelector } from 'react-redux'
import type { RootState } from '@/types/state'

interface CompanyTooltipProps {
  mouseX: number
  mouseY: number
  visible: boolean
}

export const CompanyTooltip: React.FC<CompanyTooltipProps> = ({ mouseX, mouseY, visible }) => {
  const company = useSelector((state: RootState) => state.companies.hoveredCompany)
  
  // Hide tooltip on mobile devices
  const isMobile = window.innerWidth <= 640
  
  if (!visible || !company || isMobile) {
    return null
  }
  
  return (
    <div
      className="fixed pointer-events-none bg-gray-800 text-white p-3 rounded-lg shadow-lg border border-gray-600 z-[60] max-w-xs"
      style={{
        left: `${mouseX + 15}px`,
        top: `${mouseY}px`,
        transform: `translateY(-100%) ${mouseX > window.innerWidth - 250 ? 'translateX(-105%)' : ''}`,
        transition: 'transform 0.1s ease-out, opacity 0.1s ease-out',
        opacity: 1,
      }}
    >
      <div className="space-y-1">
        <div className="font-bold text-cyan-400 text-lg">{company.ticker}</div>
        <div className="text-sm text-gray-300 truncate">{company.company_name || company.name}</div>
        {company.latest_close > 0 && (
          <div className="text-lg font-mono text-green-400">
            ${company.latest_close.toFixed(2)}
          </div>
        )}
        {company.latest_volume > 0 && (
          <div className="text-xs text-gray-400">
            Vol: {(company.latest_volume / 1e6).toFixed(1)}M
          </div>
        )}
        {company.address && (
          <div className="text-xs text-gray-500 truncate">
            {company.address}
          </div>
        )}
      </div>
    </div>
  )
}