import * as Comlink from 'comlink'

interface CompanyPoint {
  ticker: string;
  x: number;
  y: number; 
  z: number;
}

interface Point2D {
  x: number;
  y: number;
}

class SpatialOperations {
  
  constructor() {}

  initializeData(): void {
    // no-op
  }

  selectInRectangle(min: Point2D, max: Point2D, allCoords: CompanyPoint[]): CompanyPoint[] {
    const selected: CompanyPoint[] = []
    for (const company of allCoords) {
      if (
        company.x >= min.x && company.x <= max.x &&
        company.z >= min.y && company.z <= max.y
      ) {
        selected.push(company)
      }
    }
    return selected
  }

  selectInCircle(center: Point2D, radius: number, allCoords: CompanyPoint[]): CompanyPoint[] {
    const selected: CompanyPoint[] = []
    const radiusSq = radius * radius
    for (const company of allCoords) {
      const dx = company.x - center.x
      const dz = company.z - center.y
      if (dx * dx + dz * dz <= radiusSq) {
        selected.push(company)
      }
    }
    return selected
  }

  selectInPolygon(polygonPoints: Point2D[], allCoords: CompanyPoint[]): CompanyPoint[] {
    if (polygonPoints.length < 3) {
      return []
    }
    
    const selected: CompanyPoint[] = []
    for (const company of allCoords) {
      if (this.isPointInPolygon({ x: company.x, y: company.z }, polygonPoints)) {
        selected.push(company)
      }
    }
    return selected;
  }
  
  private isPointInPolygon(point: Point2D, polygon: Point2D[]): boolean {
    const { x, y } = point
    let inside = false
    for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
      const xi = polygon[i].x, yi = polygon[i].y
      const xj = polygon[j].x, yj = polygon[j].y

      const intersect = ((yi > y) !== (yj > y))
          && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)
      if (intersect) inside = !inside
    }
    return inside
  }
}

const spatialOps = new SpatialOperations()
Comlink.expose(spatialOps)