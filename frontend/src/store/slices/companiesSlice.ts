import { createSlice, PayloadAction } from '@reduxjs/toolkit'
import type { CompaniesState, CompanyFilters } from '@/types/state'
import type { CompletedMarketRecord } from '@/types/api'

const initialState: CompaniesState = {
  entities: {},
  visibleIds: [],
  selectedIds: [],
  hoveredId: null,
  hoveredCompany: null,
  filters: {},
  loading: false,
  error: null,
  lastUpdated: null
}

export const companiesSlice = createSlice({
  name: 'companies',
  initialState,
  reducers: {
    setCompanies: (state, action: PayloadAction<CompletedMarketRecord[]>) => {
      const entities: Record<string, CompletedMarketRecord> = {}
      const visibleIds: string[] = []
      
      action.payload.forEach(company => {
        entities[company.ticker] = company
        visibleIds.push(company.ticker)
      })
      
      state.entities = entities
      state.visibleIds = visibleIds
      state.loading = false
      state.error = null
      state.lastUpdated = Date.now()
    },

    updateCompany: (state, action: PayloadAction<Partial<CompletedMarketRecord> & { ticker: string }>) => {
      const { ticker, ...updates } = action.payload
      if (state.entities[ticker]) {
        state.entities[ticker] = { ...state.entities[ticker], ...updates }
      }
    },

    setVisibleCompanies: (state, action: PayloadAction<string[]>) => {
      state.visibleIds = action.payload
    },

    setHoveredCompany: (state, action: PayloadAction<string | null>) => {
      state.hoveredId = action.payload
      state.hoveredCompany = action.payload ? state.entities[action.payload] || null : null
    },

    setFilters: (state, action: PayloadAction<Partial<CompanyFilters>>) => {
      state.filters = { ...state.filters, ...action.payload }
      // Apply filters to update visible companies
      state.visibleIds = applyFilters(state.entities, state.filters)
    },

    clearFilters: (state) => {
      state.filters = {}
      state.visibleIds = Object.keys(state.entities)
    },

    setLoading: (state, action: PayloadAction<boolean>) => {
      state.loading = action.payload
      if (action.payload) {
        state.error = null
      }
    },

    setError: (state, action: PayloadAction<string | null>) => {
      state.error = action.payload
      state.loading = false
    },

    // Bulk operations for performance
    batchUpdateCompanies: (state, action: PayloadAction<Array<Partial<CompletedMarketRecord> & { ticker: string }>>) => {
      action.payload.forEach(update => {
        const { ticker, ...updates } = update
        if (state.entities[ticker]) {
          state.entities[ticker] = { ...state.entities[ticker], ...updates }
        }
      })
    }
  }
})

// Helper function to apply filters
function applyFilters(
  entities: Record<string, CompletedMarketRecord>, 
  filters: CompanyFilters
): string[] {
  return Object.keys(entities).filter(ticker => {
    const company = entities[ticker]
    
    // Sector filter
    if (filters.sectors && filters.sectors.length > 0) {
    //   if (!company.sector || !filters.sectors.includes(company.sector)) {
    //     return false
    //   }
    }
    
    // State filter
    if (filters.states && filters.states.length > 0) {
    //   if (!company.state || !filters.states.includes(company.state)) {
    //     return false
    //   }
    }
    
    // Location requirement filter
    if (filters.hasLocation) {
      if (!company.latitude || !company.longitude) {
        return false
      }
    }
    
    // Search query filter
    if (filters.searchQuery) {
      const query = filters.searchQuery.toLowerCase()
      const searchableText = `${company.ticker} ${company.company_name}`.toLowerCase()
      if (!searchableText.includes(query)) {
        return false
      }
    }
    
    return true
  })
}