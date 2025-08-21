import React from 'react'
import { useSelector } from 'react-redux'
import type { RootState } from '@/types/state'

export const SelectionOverlay: React.FC = () => {
  const selectionGeometry = useSelector((state: RootState) => state.map.selectionGeometry)
  
  if (!selectionGeometry) return null

  const { type, start, current, points } = selectionGeometry

  const renderRectangleSelection = () => {
    if (!start || !current) return null
    
    const width = Math.abs(current.x - start.x)
    const height = Math.abs(current.y - start.y)
    const left = Math.min(start.x, current.x)
    const top = Math.min(start.y, current.y)

    return (
      <div
        className="absolute border-2 border-dashed border-green-400 bg-green-400/10 pointer-events-none"
        style={{
          left: `${left}px`,
          top: `${top}px`,
          width: `${width}px`,
          height: `${height}px`,
          zIndex: 30
        }}
      />
    )
  }

  const renderCircleSelection = () => {
    if (!start || !current) return null
    
    const centerX = (start.x + current.x) / 2
    const centerY = (start.y + current.y) / 2
    const radius = Math.sqrt(
      Math.pow(current.x - start.x, 2) + 
      Math.pow(current.y - start.y, 2)
    ) / 2

    return (
      <div
        className="absolute border-2 border-dashed border-green-400 bg-green-400/10 pointer-events-none rounded-full"
        style={{
          left: `${centerX - radius}px`,
          top: `${centerY - radius}px`,
          width: `${radius * 2}px`,
          height: `${radius * 2}px`,
          zIndex: 30
        }}
      />
    )
  }

  const renderPolygonSelection = () => {
    if (!points || points.length < 2) return null

    // Create SVG path for polygon
    const pathData = points.reduce((path, point, index) => {
      const command = index === 0 ? 'M' : 'L'
      return `${path} ${command} ${point.x} ${point.y}`
    }, '') + ' Z'

    return (
      <svg
        className="absolute inset-0 pointer-events-none"
        style={{ zIndex: 30 }}
      >
        <defs>
          <pattern
            id="selection-pattern"
            patternUnits="userSpaceOnUse"
            width="4"
            height="4"
          >
            <rect width="4" height="4" fill="transparent" />
            <path d="M0,4 L4,0" stroke="#10b981" strokeWidth="0.5" />
          </pattern>
        </defs>
        <path
          d={pathData}
          fill="url(#selection-pattern)"
          stroke="#10b981"
          strokeWidth="2"
          strokeDasharray="5,5"
          fillOpacity="0.1"
        />
        
        {/* Show points */}
        {points.map((point, index) => (
          <circle
            key={index}
            cx={point.x}
            cy={point.y}
            r="3"
            fill="#10b981"
            stroke="white"
            strokeWidth="1"
          />
        ))}
      </svg>
    )
  }

  const renderLassoSelection = () => {
    if (!points || points.length < 2) return null

    // Create smooth path for lasso using bezier curves
    const pathData = points.reduce((path, point, index) => {
      if (index === 0) {
        return `M ${point.x} ${point.y}`
      } else if (index === 1) {
        return `${path} L ${point.x} ${point.y}`
      } else {
        // Use quadratic bezier curves for smoother lasso
        const prevPoint = points[index - 1]
        const controlX = (prevPoint.x + point.x) / 2
        const controlY = (prevPoint.y + point.y) / 2
        return `${path} Q ${prevPoint.x} ${prevPoint.y} ${controlX} ${controlY}`
      }
    }, '')

    return (
      <svg
        className="absolute inset-0 pointer-events-none"
        style={{ zIndex: 30 }}
      >
        <path
          d={pathData}
          fill="none"
          stroke="#10b981"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          opacity="0.8"
        />
        
        {/* Animated drawing effect */}
        <path
          d={pathData}
          fill="none"
          stroke="#22d3ee"
          strokeWidth="1"
          strokeLinecap="round"
          strokeDasharray="3,3"
          opacity="0.6"
        >
          <animate
            attributeName="stroke-dashoffset"
            values="0;-6"
            dur="0.5s"
            repeatCount="indefinite"
          />
        </path>
      </svg>
    )
  }

  switch (type) {
    case 'rect':
      return renderRectangleSelection()
    case 'circle':
      return renderCircleSelection()
    case 'polygon':
      return renderPolygonSelection()
    case 'lasso':
      return renderLassoSelection()
    default:
      return null
  }
}
