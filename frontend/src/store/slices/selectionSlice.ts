import { createSlice, PayloadAction } from '@reduxjs/toolkit'
import type { 
  SelectionState, 
  SelectionHistoryEntry, 
  SelectionMode,
  SelectionGeometry 
} from '@/types/state'
import type { RegionalStats } from '@/types/api'

const initialState: SelectionState = {
  selectedCompanies: new Set(),
  selectionStats: null,
  selectionHistory: [],
  isSelecting: false,
  selectionMode: 'pan'
}

export const selectionSlice = createSlice({
  name: 'selection',
  initialState,
  reducers: {
    setSelectedCompanies: (state, action: PayloadAction<string[]>) => {
      state.selectedCompanies = new Set(action.payload)
    },

    addToSelection: (state, action: PayloadAction<string[]>) => {
      action.payload.forEach(ticker => state.selectedCompanies.add(ticker))
    },

    removeFromSelection: (state, action: PayloadAction<string[]>) => {
      action.payload.forEach(ticker => state.selectedCompanies.delete(ticker))
    },

    toggleSelection: (state, action: PayloadAction<string>) => {
      if (state.selectedCompanies.has(action.payload)) {
        state.selectedCompanies.delete(action.payload)
      } else {
        state.selectedCompanies.add(action.payload)
      }
    },

    clearSelection: (state) => {
      state.selectedCompanies.clear()
      state.selectionStats = null
    },

    setSelectionStats: (state, action: PayloadAction<RegionalStats | null>) => {
      state.selectionStats = action.payload
    },

    setIsSelecting: (state, action: PayloadAction<boolean>) => {
      state.isSelecting = action.payload
    },

    addToSelectionHistory: (state, action: PayloadAction<{
      companyIds: string[]
      stats: RegionalStats
      mode: SelectionMode
      geometry: SelectionGeometry
    }>) => {
      const { companyIds, stats, mode, geometry } = action.payload
      
      const historyEntry: SelectionHistoryEntry = {
        id: `selection_${Date.now()}`,
        timestamp: Date.now(),
        companyIds,
        stats,
        mode,
        geometry
      }
      
      state.selectionHistory.unshift(historyEntry)
      
      if (state.selectionHistory.length > 50) {
        state.selectionHistory = state.selectionHistory.slice(0, 50)
      }
    },

    restoreSelectionFromHistory: (state, action: PayloadAction<string>) => {
      const historyId = action.payload
      const historyEntry = state.selectionHistory.find(entry => entry.id === historyId)
      
      if (historyEntry) {
        state.selectedCompanies = new Set(historyEntry.companyIds)
        state.selectionStats = historyEntry.stats
      }
    },

    removeFromSelectionHistory: (state, action: PayloadAction<string>) => {
      state.selectionHistory = state.selectionHistory.filter(
        entry => entry.id !== action.payload
      )
    },

    clearSelectionHistory: (state) => {
      state.selectionHistory = []
    },
  }
})

export const selectSelectedCompaniesArray = (state: { selection: SelectionState }) =>
  Array.from(state.selection.selectedCompanies)