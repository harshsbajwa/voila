import React from 'react'
import { useSelectionHistory } from '@/hooks/useSelection'

const SelectionHistory: React.FC = () => {
  const { history, removeFromHistory, clearHistory } = useSelectionHistory()

  if (history.length === 0) return null

  return (
    <div className="fixed bottom-4 right-4 bg-gray-800 p-4 rounded-lg max-w-xs z-40">
      <div className="flex justify-between items-center mb-2">
        <h3 className="text-sm font-semibold">Selection History</h3>
        <button
          onClick={clearHistory}
          className="text-xs text-red-400 hover:text-red-300"
        >
          Clear
        </button>
      </div>
      
      <div className="space-y-1 max-h-32 overflow-y-auto scrollbar-thin">
        {history.slice(0, 5).map(entry => (
          <div
            key={entry.id}
            className="flex justify-between items-center text-xs bg-gray-700 p-2 rounded"
          >
            <span>{entry.companyIds.length} companies</span>
            <button
              onClick={() => removeFromHistory(entry.id)}
              className="text-red-400 hover:text-red-300 ml-2"
            >
              ×
            </button>
          </div>
        ))}
      </div>
    </div>
  )
}

export default SelectionHistory