import { createSlice, PayloadAction } from '@reduxjs/toolkit'
import type { MapState, SelectionMode, SelectionGeometry } from '@/types/state'

const initialState: MapState = {
  camera: {
    position: { x: 0, y: 10, z: 0 },
    zoom: 1,
    target: { x: 0, y: 0, z: 0 }
  },
  viewport: {
    bounds: {
      north: 49.0,
      south: 25.0,
      east: -66.0,
      west: -125.0
    },
    center: { lat: 39.5, lng: -98.35 }, // Geographic center of US
    zoomLevel: 4
  },
  selectionMode: 'pan',
  selectionGeometry: null,
  lodLevel: 1,
  performanceMode: 'high'
}

export const mapSlice = createSlice({
  name: 'map',
  initialState,
  reducers: {
    updateCamera: (state, action: PayloadAction<{
      position?: { x: number; y: number; z: number }
      zoom?: number
      target?: { x: number; y: number; z: number }
    }>) => {
      if (action.payload.position) {
        state.camera.position = action.payload.position
      }
      if (action.payload.zoom !== undefined) {
        state.camera.zoom = action.payload.zoom
      }
      if (action.payload.target) {
        state.camera.target = action.payload.target
      }
    },

    setViewport: (state, action: PayloadAction<Partial<MapState['viewport']>>) => {
      state.viewport = { ...state.viewport, ...action.payload }
    },

    setSelectionMode: (state, action: PayloadAction<SelectionMode>) => {
      state.selectionMode = action.payload
      // Clear selection geometry when changing modes
      if (action.payload === 'pan') {
        state.selectionGeometry = null
      }
    },

    setSelectionGeometry: (state, action: PayloadAction<SelectionGeometry | null>) => {
      state.selectionGeometry = action.payload
    },

    setLODLevel: (state, action: PayloadAction<number>) => {
      state.lodLevel = Math.max(0, Math.min(3, action.payload))
    },

    setPerformanceMode: (state, action: PayloadAction<'high' | 'medium' | 'low'>) => {
      state.performanceMode = action.payload
      
      // Automatically adjust LOD based on performance mode
      switch (action.payload) {
        case 'high':
          state.lodLevel = 0
          break
        case 'medium':
          state.lodLevel = 1
          break
        case 'low':
          state.lodLevel = 2
          break
      }
    },

    // Camera animation and movement
    panCamera: (state, action: PayloadAction<{ deltaX: number; deltaY: number }>) => {
      const { deltaX, deltaY } = action.payload
      const panSpeed = 0.05 / state.camera.zoom;
      
      // Update camera position
      state.camera.position.x -= deltaX * panSpeed
      state.camera.position.z -= deltaY * panSpeed
    },

    zoomCamera: (state, action: PayloadAction<number>) => {
      const zoomFactor = action.payload
      const newZoom = Math.max(0.1, Math.min(10, state.camera.zoom * zoomFactor))
      state.camera.zoom = newZoom
      
      // Update viewport zoom level
      state.viewport.zoomLevel = Math.log2(newZoom) + 4
    },

    // Viewport bounds calculation
    updateViewportBounds: (state, action: PayloadAction<{
      north: number
      south: number
      east: number
      west: number
    }>) => {
      state.viewport.bounds = action.payload
    },

    // Reset camera to default position
    resetCamera: (state) => {
      state.camera.position = { x: 0, y: 10, z: 0 }
      state.camera.zoom = 1
      state.camera.target = { x: 0, y: 0, z: 0 }
      state.viewport.center = { lat: 39.5, lng: -98.35 }
      state.viewport.zoomLevel = 4
    },

    // Focus camera on specific region
    focusOnRegion: (state, action: PayloadAction<{
      center: { lat: number; lng: number }
      zoom?: number
    }>) => {
      const { center, zoom = 6 } = action.payload
      
      state.viewport.center = center
      state.viewport.zoomLevel = zoom
    }
  }
})