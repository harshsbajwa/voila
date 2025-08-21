// Manager for spatial calculations Web Worker with Comlink
import * as Comlink from 'comlink'
import * as THREE from 'three'
import { debug } from './debug'

// Types for communication with worker
interface SpatialWorkerAPI {
  initializeData(companies: any[]): Promise<void>
  selectInRectangle(min: THREE.Vector2, max: THREE.Vector2, allCoords: any[]): Promise<any[]>
  selectInCircle(center: THREE.Vector2, radius: number, allCoords: any[]): Promise<any[]>
  selectInPolygon(polygonPoints: THREE.Vector2[], allCoords: any[]): Promise<any[]>
}

export class SpatialWorkerManager {
  private worker: Worker | null = null
  private spatialAPI: Comlink.Remote<SpatialWorkerAPI> | null = null
  private initializationPromise: Promise<void> | null = null

  constructor() {
    this.initializeWorker()
  }

  private async initializeWorker(): Promise<void> {
    try {
      this.worker = new Worker(
        new URL('../workers/spatialWorker.ts', import.meta.url),
        { type: 'module' }
      )
      this.spatialAPI = Comlink.wrap<SpatialWorkerAPI>(this.worker)
      debug.log('Spatial worker initialized successfully')
    } catch (error) {
      this.spatialAPI = null
    }
  }

  public initializeData(companies: any[]): Promise<void> {
    if (this.initializationPromise) {
      return this.initializationPromise
    }
    const doInit = async () => {
        if (!this.spatialAPI) {
            debug.warn('Spatial worker not available. Selections will be disabled.')
            return
        }
        try {
            await this.spatialAPI.initializeData(companies)
            debug.log(`Spatial data initialized in worker with ${companies.length} companies`)
        } catch (error) {
        }
    }
    this.initializationPromise = doInit()
    return this.initializationPromise
  }
  
  public async selectInRectangle(min: THREE.Vector2, max: THREE.Vector2, allCoords: any[]): Promise<any[]> {
    if (!this.spatialAPI) return []
    try {
      return await this.spatialAPI.selectInRectangle(min, max, allCoords)
    } catch (error) {
      return []
    }
  }

  public async selectInCircle(center: THREE.Vector2, radius: number, allCoords: any[]): Promise<any[]> {
    if (!this.spatialAPI) return []
    try {
      return await this.spatialAPI.selectInCircle(center, radius, allCoords)
    } catch (error) {
      return []
    }
  }

  public async selectInPolygon(polygonPoints: THREE.Vector2[], allCoords: any[]): Promise<any[]> {
    if (!this.spatialAPI) return []
    try {
      return await this.spatialAPI.selectInPolygon(polygonPoints, allCoords)
    } catch (error) {
      return []
    }
  }

  public dispose(): void {
    if (this.worker) {
      this.worker.terminate()
      this.worker = null
    }
    this.spatialAPI = null
    this.initializationPromise = null
    debug.log('Spatial worker disposed.')
  }
}

// Singleton instance
export const spatialWorkerManager = new SpatialWorkerManager()