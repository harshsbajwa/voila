import React, { useState } from 'react'
import { API_BASE_URL, API_ENDPOINTS } from '@/config/api'
import { debug } from '@/utils/debug'

const TestDataLoaderComponent: React.FC = () => {
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const testBasicCompanies = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE_URL}${API_ENDPOINTS.companies}?limit=5`)
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const result = await response.json()
      setData(result)
      debug.log('Basic companies data:', result)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
      debug.error('Failed to load basic companies:', err)
    } finally {
      setLoading(false)
    }
  }

  const testBulkMarketData = async () => {
    setLoading(true)
    setError(null)
    try {
      // Test with specific tickers
      const testTickers = 'AAPL,MSFT,NVDA'
      const response = await fetch(`${API_BASE_URL}${API_ENDPOINTS.bulkCompleteMarketData}?tickers=${testTickers}&limit=3`)
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const result = await response.json()
      setData(result)
      debug.log('Bulk market data:', result)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
      debug.error('Failed to load bulk market data:', err)
    } finally {
      setLoading(false)
    }
  }

  const testMarketOverview = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE_URL}${API_ENDPOINTS.marketOverview}`)
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const result = await response.json()
      setData(result)
      debug.log('Market overview:', result)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
      debug.error('Failed to load market overview:', err)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{
      position: 'fixed',
      bottom: '10px',
      right: '10px',
      width: '350px',
      backgroundColor: 'rgba(0, 0, 0, 0.95)',
      color: '#00ff00',
      fontFamily: 'monospace',
      fontSize: '11px',
      padding: '10px',
      borderRadius: '5px',
      border: '1px solid #00ff00',
      zIndex: 10001,
      maxHeight: '300px',
      overflowY: 'auto'
    }}>
      <h4 style={{ color: '#ffff00', marginTop: 0 }}>🧪 API Test</h4>
      
      <div style={{ marginBottom: '10px' }}>
        <button 
          onClick={testBasicCompanies}
          disabled={loading}
          style={{
            padding: '2px 8px',
            marginRight: '5px',
            backgroundColor: '#004400',
            border: '1px solid #00ff00',
            color: '#00ff00',
            cursor: loading ? 'wait' : 'pointer',
            fontSize: '10px'
          }}
        >
          Basic Companies
        </button>
        <button 
          onClick={testBulkMarketData}
          disabled={loading}
          style={{
            padding: '2px 8px',
            marginRight: '5px',
            backgroundColor: '#004400',
            border: '1px solid #00ff00',
            color: '#00ff00',
            cursor: loading ? 'wait' : 'pointer',
            fontSize: '10px'
          }}
        >
          Bulk Market
        </button>
        <button 
          onClick={testMarketOverview}
          disabled={loading}
          style={{
            padding: '2px 8px',
            backgroundColor: '#004400',
            border: '1px solid #00ff00',
            color: '#00ff00',
            cursor: loading ? 'wait' : 'pointer',
            fontSize: '10px'
          }}
        >
          Overview
        </button>
      </div>

      {loading && <div style={{ color: '#ffff00' }}>Loading...</div>}
      
      {error && (
        <div style={{ color: '#ff0000', marginBottom: '10px' }}>
          Error: {error}
        </div>
      )}
      
      {data && (
        <div>
          <div style={{ color: '#00ffff', marginBottom: '5px' }}>
            Response ({Array.isArray(data) ? `${data.length} items` : 'object'}):
          </div>
          <pre style={{ 
            fontSize: '10px', 
            maxHeight: '200px', 
            overflowY: 'auto',
            backgroundColor: 'rgba(0, 50, 0, 0.5)',
            padding: '5px',
            borderRadius: '3px'
          }}>
            {JSON.stringify(data, null, 2)}
          </pre>
        </div>
      )}
    </div>
  )
}

// Only export if debug mode is enabled
export const TestDataLoader = import.meta.env.VITE_ENABLE_TEST_LOADER === 'true' 
  ? TestDataLoaderComponent 
  : () => null