import React, { useEffect, useState } from 'react'
import { useSelector } from 'react-redux'
import type { RootState } from '@/types/state'

export const DataInspector: React.FC = () => {
  const companies = useSelector((state: RootState) => state.companies.entities)
  const [sampleData, setSampleData] = useState<any[]>([])
  const [stats, setStats] = useState<any>({})

  useEffect(() => {
    const companiesArray = Object.values(companies)
    
    // Get first 5 companies as sample
    const sample = companiesArray.slice(0, 5)
    setSampleData(sample)

    // Calculate statistics
    const withCoords = companiesArray.filter(c => c.latitude && c.longitude)
    const withPrice = companiesArray.filter(c => c.latest_close && c.latest_close > 0)
    
    // Check coordinate bounds
    const coordBounds = withCoords.reduce((acc, c) => {
      if (!acc.minLat || (c.latitude !== undefined && c.latitude < acc.minLat)) acc.minLat = c.latitude ?? null
      if (!acc.maxLat || (c.latitude !== undefined && c.latitude > acc.maxLat)) acc.maxLat = c.latitude ?? null
      if (!acc.minLng || (c.longitude !== undefined && c.longitude < acc.minLng)) acc.minLng = c.longitude ?? null
      if (!acc.maxLng || (c.longitude !== undefined && c.longitude > acc.maxLng)) acc.maxLng = c.longitude ?? null
      return acc
    }, { minLat: null as number | null, maxLat: null as number | null, minLng: null as number | null, maxLng: null as number | null })

    // Find outliers (likely bad coordinates)
    const outliers = withCoords.filter(c => {
      // US bounds roughly: lat 24-49 (continental), lng -125 to -66
      // With Alaska/Hawaii: lat 18-72, lng -180 to -66
      return c.latitude! < 18 || c.latitude! > 72 || 
             c.longitude! < -180 || c.longitude! > -66
    })

    setStats({
      total: companiesArray.length,
      withCoords: withCoords.length,
      withPrice: withPrice.length,
      coordBounds,
      outliers: outliers.map(c => ({
        ticker: c.ticker,
        name: c.company_name,
        lat: c.latitude,
        lng: c.longitude
      })),
      avgPrice: withPrice.length > 0 
        ? withPrice.reduce((sum, c) => sum + (c.latest_close || 0), 0) / withPrice.length
        : 0
    })
  }, [companies])

  return (
    <div style={{
      position: 'fixed',
      top: '10px',
      right: '10px',
      width: '400px',
      maxHeight: '80vh',
      overflowY: 'auto',
      backgroundColor: 'rgba(0, 0, 0, 0.9)',
      color: '#00ff00',
      fontFamily: 'monospace',
      fontSize: '12px',
      padding: '10px',
      borderRadius: '5px',
      border: '1px solid #00ff00',
      zIndex: 10000
    }}>
      <h3 style={{ color: '#00ffff', marginTop: 0 }}>🔍 Data Inspector</h3>
      
      <div style={{ marginBottom: '15px' }}>
        <h4 style={{ color: '#ffff00' }}>Statistics:</h4>
        <div>Total Companies: {stats.total}</div>
        <div>With Coordinates: {stats.withCoords} ({((stats.withCoords / stats.total) * 100).toFixed(1)}%)</div>
        <div>With Price Data: {stats.withPrice} ({((stats.withPrice / stats.total) * 100).toFixed(1)}%)</div>
        <div>Average Price: ${stats.avgPrice?.toFixed(2)}</div>
      </div>

      <div style={{ marginBottom: '15px' }}>
        <h4 style={{ color: '#ffff00' }}>Coordinate Bounds:</h4>
        {stats.coordBounds && (
          <>
            <div>Lat: {stats.coordBounds.minLat?.toFixed(2)} to {stats.coordBounds.maxLat?.toFixed(2)}</div>
            <div>Lng: {stats.coordBounds.minLng?.toFixed(2)} to {stats.coordBounds.maxLng?.toFixed(2)}</div>
          </>
        )}
      </div>

      {stats.outliers && stats.outliers.length > 0 && (
        <div style={{ marginBottom: '15px' }}>
          <h4 style={{ color: '#ff0000' }}>⚠️ Coordinate Outliers ({stats.outliers.length}):</h4>
          {stats.outliers.slice(0, 5).map((o: any, i: number) => (
            <div key={i} style={{ fontSize: '10px', marginLeft: '10px' }}>
              {o.ticker}: {o.name}
              <br />
              Lat: {o.lat}, Lng: {o.lng}
            </div>
          ))}
        </div>
      )}

      <div>
        <h4 style={{ color: '#ffff00' }}>Sample Data (First 5):</h4>
        {sampleData.map((company, i) => (
          <div key={i} style={{ marginBottom: '10px', borderBottom: '1px solid #333', paddingBottom: '5px' }}>
            <div style={{ color: '#00ffff' }}>{company.ticker}: {company.company_name || company.name}</div>
            <div style={{ fontSize: '10px', marginLeft: '10px' }}>
              <div>Price: ${company.latest_close || 'N/A'}</div>
              <div>Lat: {company.latitude || 'N/A'}</div>
              <div>Lng: {company.longitude || 'N/A'}</div>
              <div>Address: {company.address || 'N/A'}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}