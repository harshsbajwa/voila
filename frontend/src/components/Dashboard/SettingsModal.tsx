import React from 'react'
import { useSelector, useDispatch } from 'react-redux'
import type { RootState } from '@/types/state'

const SettingsModal: React.FC = () => {
  const dispatch = useDispatch()
  const isOpen = useSelector((state: RootState) => state.ui.settingsModalOpen)
  const theme = useSelector((state: RootState) => state.ui.theme)
  const performanceMode = useSelector((state: RootState) => state.map.performanceMode)

  if (!isOpen) return null

  const handleClose = () => {
    dispatch({ type: 'ui/closeSettingsModal' })
  }

  const handleThemeChange = (newTheme: 'dark' | 'light') => {
    dispatch({ type: 'ui/setTheme', payload: newTheme })
  }

  const handlePerformanceModeChange = (mode: 'high' | 'medium' | 'low') => {
    dispatch({ type: 'map/setPerformanceMode', payload: mode })
  }

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-gray-800 p-6 rounded-lg max-w-md w-full mx-4">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-lg font-semibold">Settings</h2>
          <button
            onClick={handleClose}
            className="text-gray-400 hover:text-white"
          >
            ✕
          </button>
        </div>
        
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium mb-2">Theme</label>
            <div className="space-x-2">
              <button
                onClick={() => handleThemeChange('dark')}
                className={`px-3 py-1 rounded text-sm ${
                  theme === 'dark' ? 'bg-blue-600' : 'bg-gray-600'
                }`}
              >
                Dark
              </button>
              <button
                onClick={() => handleThemeChange('light')}
                className={`px-3 py-1 rounded text-sm ${
                  theme === 'light' ? 'bg-blue-600' : 'bg-gray-600'
                }`}
              >
                Light
              </button>
            </div>
          </div>
          
          <div>
            <label className="block text-sm font-medium mb-2">Performance Mode</label>
            <div className="space-x-2">
              {(['high', 'medium', 'low'] as const).map(mode => (
                <button
                  key={mode}
                  onClick={() => handlePerformanceModeChange(mode)}
                  className={`px-3 py-1 rounded text-sm capitalize ${
                    performanceMode === mode ? 'bg-green-600' : 'bg-gray-600'
                  }`}
                >
                  {mode}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default SettingsModal