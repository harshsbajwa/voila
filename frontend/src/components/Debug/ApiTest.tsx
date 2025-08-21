import React, { useEffect, useState } from 'react'
import { debug } from '@/utils/debug'

export const ApiTest: React.FC = () => {
  const [status, setStatus] = useState<string>('Testing...')
  
  useEffect(() => {
    const testApi = async () => {
      try {
        debug.log('🧪 Testing API connection...')
        
        // Test health endpoint
        const healthResponse = await fetch('http://localhost:8000/health')
        debug.log('🧪 Health response:', healthResponse.status)
        
        if (!healthResponse.ok) {
          setStatus('❌ API health check failed')
          return
        }
        
        // Test companies endpoint
        const companiesResponse = await fetch('http://localhost:8000/api/v1/data/companies?limit=5&has_location=true')
        debug.log('🧪 Companies response:', companiesResponse.status)
        
        if (!companiesResponse.ok) {
          setStatus('❌ Companies API failed')
          return
        }
        
        const companies = await companiesResponse.json()
        debug.log('🧪 Companies data:', companies)
        setStatus(`✅ API working! Found ${companies.length} companies`)
        
      } catch (error) {
        debug.error('🧪 API test failed:', error)
        setStatus('❌ API connection failed')
      }
    }
    
    testApi()
  }, [])
  
  return (
    <div style={{ 
      position: 'fixed', 
      bottom: '10px', 
      left: '220px', 
      background: 'rgba(0,0,0,0.8)', 
      color: 'white', 
      padding: '10px', 
      borderRadius: '5px',
      fontSize: '12px',
      zIndex: 9999
    }}>
      API Status: {status}
    </div>
  )
}