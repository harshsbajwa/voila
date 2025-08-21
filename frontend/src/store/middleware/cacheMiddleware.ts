import type { Middleware, AnyAction } from '@reduxjs/toolkit'
import { openDB, type DBSchema, type IDBPDatabase } from 'idb'
import type { RootState } from '@/types/state'
import type { CompletedMarketRecord, OHLCVResponseRecord, RegionalStats } from '@/types/api'
import type { SelectionHistoryEntry } from '@/types/state'

// IndexedDB Schema
interface CacheDB extends DBSchema {
  companies: {
    key: string
    value: {
      ticker: string
      data: CompletedMarketRecord
      timestamp: number
      ttl: number
    }
    indexes: { timestamp: number }
  }
  chartData: {
    key: string
    value: {
      key: string
      data: OHLCVResponseRecord[]
      timestamp: number
      ttl: number
    }
    indexes: { timestamp: number }
  }
  selections: {
    key: string
    value: {
      id: string
      companyIds: string[]
      stats: unknown
      timestamp: number
    }
    indexes: { timestamp: number }
  }
}

let db: IDBPDatabase<CacheDB> | null = null

// Initialize IndexedDB
const initDB = async () => {
  if (db) return db
  
  try {
    db = await openDB<CacheDB>('market-dashboard-cache', 1, {
      upgrade(db) {
        // Companies cache
        if (!db.objectStoreNames.contains('companies')) {
          const companiesStore = db.createObjectStore('companies', { keyPath: 'ticker' })
          companiesStore.createIndex('timestamp', 'timestamp')
        }
        
        // Chart data cache
        if (!db.objectStoreNames.contains('chartData')) {
          const chartStore = db.createObjectStore('chartData', { keyPath: 'key' })
          chartStore.createIndex('timestamp', 'timestamp')
        }
        
        // Selection history
        if (!db.objectStoreNames.contains('selections')) {
          const selectionsStore = db.createObjectStore('selections', { keyPath: 'id' })
          selectionsStore.createIndex('timestamp', 'timestamp')
        }
      }
    })
    
    // Clean up expired entries on startup
    cleanupExpiredEntries()
    
    return db
  } catch (error) {
    console.error('Failed to initialize IndexedDB:', error)
    return null
  }
}

// Clean up expired cache entries
const cleanupExpiredEntries = async () => {
  if (!db) return
  
  const now = Date.now()
  
  try {
    // Clean companies cache
    const companiesTx = db.transaction('companies', 'readwrite')
    const companiesStore = companiesTx.objectStore('companies')
    const companiesIndex = companiesStore.index('timestamp')
    let companiesCursor = await companiesIndex.openCursor()
    
    while (companiesCursor) {
      const entry = companiesCursor.value
      if (now > entry.timestamp + entry.ttl) {
        await companiesCursor.delete()
      }
      companiesCursor = await companiesCursor.continue()
    }
    await companiesTx.done
    
    // Clean chart data cache
    const chartTx = db.transaction('chartData', 'readwrite')
    const chartStore = chartTx.objectStore('chartData')
    const chartIndex = chartStore.index('timestamp')
    let chartCursor = await chartIndex.openCursor()
    
    while (chartCursor) {
      const entry = chartCursor.value
      if (now > entry.timestamp + entry.ttl) {
        await chartCursor.delete()
      }
      chartCursor = await chartCursor.continue()
    }
    await chartTx.done
  } catch (error) {
    console.error('Failed to cleanup expired cache entries:', error)
  }
}

export const cacheMiddleware: Middleware<{}, RootState> = (store) => (next) => (action: unknown) => {
  // Initialize DB
  initDB()
  const result = next(action)
  
  // Cache specific actions
  switch ((action as AnyAction).type) {
    case 'companies/setCompanies':
      cacheCompanies((action as AnyAction).payload)
      break
      
    case 'charts/setChartData':
      const state = store.getState()
      if (state.charts.activeChart) {
        cacheChartData(state.charts.activeChart.ticker, (action as AnyAction).payload)
      }
      break
      
    case 'selection/addToSelectionHistory':
      cacheSelection((action as AnyAction).payload)
      break
  }
  
  return result
}

// Cache functions
const cacheCompanies = async (companies: CompletedMarketRecord[]) => {
  if (!db) return
  
  try {
    const tx = db.transaction('companies', 'readwrite')
    const store = tx.objectStore('companies')
    
    const timestamp = Date.now()
    const ttl = 24 * 60 * 60 * 1000 // 24 hours
    
    for (const company of companies) {
      await store.put({
        ticker: company.ticker,
        data: company,
        timestamp,
        ttl
      })
    }
    
    await tx.done
  } catch (error) {
    console.error('Failed to cache companies:', error)
  }
}

const cacheChartData = async (ticker: string, data: OHLCVResponseRecord[]) => {
  if (!db) return
  
  try {
    const key = `chart_${ticker}` // Use a stable key to overwrite old data
    const timestamp = Date.now()
    const ttl = 60 * 60 * 1000 // 1 hour
    
    await db.put('chartData', {
      key,
      data,
      timestamp,
      ttl
    })
  } catch (error) {
    console.error('Failed to cache chart data:', error)
  }
}

const cacheSelection = async (selection: SelectionHistoryEntry) => {
  if (!db) return
  
  try {
    await db.put('selections', {
      id: selection.id,
      companyIds: selection.companyIds,
      stats: selection.stats,
      timestamp: Date.now()
    })
  } catch (error) {
    console.error('Failed to cache selection:', error)
  }
}

// Cache retrieval functions
export const getCachedCompanies = async (): Promise<CompletedMarketRecord[] | null> => {
  if (!db) return null
  
  try {
    const tx = db.transaction('companies', 'readonly')
    const store = tx.objectStore('companies')
    const entries = await store.getAll()
    
    const now = Date.now()
    const validEntries = entries.filter(entry => 
      now <= entry.timestamp + entry.ttl
    )
    
    return validEntries.map(entry => entry.data)
  } catch (error) {
    console.error('Failed to retrieve cached companies:', error)
    return null
  }
}

export const getCachedChartData = async (ticker: string): Promise<OHLCVResponseRecord[] | null> => {
  if (!db) return null
  
  try {
    const key = `chart_${ticker}`;
    const entry = await db.get('chartData', key);

    if (entry) {
        const now = Date.now();
        if (now <= entry.timestamp + entry.ttl) {
            return entry.data;
        }
    }
    
    return null
  } catch (error) {
    console.error('Failed to retrieve cached chart data:', error)
    return null
  }
}

export const getCachedSelections = async (): Promise<SelectionHistoryEntry[] | null> => {
  if (!db) return null
  
  try {
    const tx = db.transaction('selections', 'readonly')
    const store = tx.objectStore('selections')
    const index = store.index('timestamp')
    
    const entries = await index.getAll()
    
    // Return most recent selections first
    return entries.sort((a, b) => b.timestamp - a.timestamp).map(entry => ({
      id: entry.id,
      timestamp: entry.timestamp,
      companyIds: entry.companyIds,
      stats: entry.stats as RegionalStats,
      mode: 'pan' as const, // Default mode for cached selections
      geometry: { type: 'pan' } // Default geometry
    }))
  } catch (error) {
    console.error('Failed to retrieve cached selections:', error)
    return null
  }
}

// Cache statistics for monitoring
export const getCacheStats = async () => {
  if (!db) return null
  
  try {
    const tx = db.transaction(['companies', 'chartData', 'selections'], 'readonly')
    
    const companiesCount = await tx.objectStore('companies').count()
    const chartDataCount = await tx.objectStore('chartData').count()
    const selectionsCount = await tx.objectStore('selections').count()
    
    return {
      companiesCount,
      chartDataCount,
      selectionsCount,
      totalEntries: companiesCount + chartDataCount + selectionsCount
    }
  } catch (error) {
    console.error('Failed to get cache stats:', error)
    return null
  }
}

// Manual cache clearing
export const clearCache = async () => {
  if (!db) return
  
  try {
    const tx = db.transaction(['companies', 'chartData', 'selections'], 'readwrite')
    
    await tx.objectStore('companies').clear()
    await tx.objectStore('chartData').clear()
    await tx.objectStore('selections').clear()
    
    await tx.done
    
  } catch (error) {
    console.error('Failed to clear cache:', error)
  }
}