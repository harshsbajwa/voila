import React, { useRef, useEffect, useState, useCallback } from 'react'
import * as THREE from 'three'
import { useThreeScene } from '@/hooks/useThreeJS'
import { useSelection } from '@/hooks/useSelection'
import { useSelector, useDispatch } from 'react-redux'
import type { RootState } from '@/types/state'
import { SelectionOverlay } from './SelectionOverlay'
import { LoadingIndicator } from '../UI/LoadingIndicator'
import { CompanyTooltip } from './CompanyTooltip'

interface MapRendererProps {
  className?: string
}

type Point2D = { x: number, y: number };

export const MapRenderer: React.FC<MapRendererProps> = ({ className }) => {
  const containerRef = useRef<HTMLDivElement>(null)
  const dispatch = useDispatch()
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 })
  const isDraggingRef = useRef(false);

  const {
    sceneReady,
    animate,
    handlePan,
    handleZoom,
    handleMouseMove: threeMouseMove,
    unprojectScreenCoords,
    getProjectedCoords
  } = useThreeScene(containerRef)

  const { selectionMode, selectRectangle, selectCircle, selectPolygon } = useSelection()

  const performanceMode = useSelector((state: RootState) => state.map.performanceMode)
  const hoveredCompanyId = useSelector((state: RootState) => state.companies.hoveredCompany)

  useEffect(() => {
    if (!sceneReady) return
    let animationId: number
    const animateLoop = () => {
      animate()
      animationId = requestAnimationFrame(animateLoop)
    }
    animationId = requestAnimationFrame(animateLoop)
    return () => cancelAnimationFrame(animationId)
  }, [sceneReady, animate])

  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    let isSelecting = false
    let selectionStart = { x: 0, y: 0 }
    let selectionPoints: Point2D[] = []
    
    // Touch state
    let lastTouchPos = { x: 0, y: 0 }
    let lastTouchDistance = 0
    let touchStartTime = 0
    
    const getEventPos = (e: MouseEvent | TouchEvent): { x: number, y: number } => {
      if ('touches' in e) {
        const touch = e.touches[0] || e.changedTouches[0]
        return { x: touch.clientX, y: touch.clientY }
      }
      return { x: e.clientX, y: e.clientY }
    }
    
    const getTouchDistance = (e: TouchEvent): number => {
      if (e.touches.length < 2) return 0
      const touch1 = e.touches[0]
      const touch2 = e.touches[1]
      return Math.sqrt(
        Math.pow(touch2.clientX - touch1.clientX, 2) +
        Math.pow(touch2.clientY - touch1.clientY, 2)
      )
    }
    
    const handlePointerDown = (e: MouseEvent | TouchEvent) => {
      e.preventDefault()
      const pos = getEventPos(e)
      
      if ('touches' in e) {
        touchStartTime = Date.now()
        lastTouchPos = pos
        if (e.touches.length === 2) {
          lastTouchDistance = getTouchDistance(e)
          // Don't enable dragging for two-finger gestures (pinch)
          return
        }
      }
      
      if (selectionMode === 'pan' || 'touches' in e) {
        isDraggingRef.current = true
        container.style.cursor = 'grabbing'
      } else {
        isSelecting = true
        selectionStart = pos
        selectionPoints = [pos]
      }
    }
    
    const handlePointerMove = (e: MouseEvent | TouchEvent) => {
      e.preventDefault()
      const pos = getEventPos(e)
      setMousePos(pos)
      
      if ('touches' in e) {
        // Handle touch move
        if (e.touches.length === 1 && isDraggingRef.current) {
          // Single touch pan with reduced sensitivity
          const deltaX = pos.x - lastTouchPos.x
          const deltaY = pos.y - lastTouchPos.y
          
          // Scale down touch sensitivity extremely for mobile (much gentler than mouse)
          const touchSensitivity = 0.5
          handlePan(deltaX * touchSensitivity, deltaY * touchSensitivity)
          lastTouchPos = pos
        } else if (e.touches.length === 2) {
          // Disable panning when pinching
          isDraggingRef.current = false
          
          // Pinch to zoom
          const currentDistance = getTouchDistance(e)
          if (lastTouchDistance > 0) {
            const deltaDistance = currentDistance - lastTouchDistance
            // Much more aggressive zoom for mobile (pinch gestures should zoom significantly more)
            const zoomDelta = deltaDistance * 0.05
            const centerX = (e.touches[0].clientX + e.touches[1].clientX) / 2
            const centerY = (e.touches[0].clientY + e.touches[1].clientY) / 2
            handleZoom(zoomDelta, centerX, centerY)
          }
          lastTouchDistance = currentDistance
        } else if (e.touches.length === 0) {
          // All touches ended
          isDraggingRef.current = false
          lastTouchDistance = 0
        }
      } else {
        // Handle mouse move
        threeMouseMove(e)
        
        if (isDraggingRef.current) {
          handlePan(e.movementX, e.movementY)
        } else if (isSelecting) {
          if (selectionMode === 'polygon') {
              selectionPoints.push(pos)
          }
          dispatch({
            type: 'map/setSelectionGeometry',
            payload: {
              type: selectionMode,
              start: selectionStart,
              current: pos,
              points: selectionMode === 'polygon' ? selectionPoints : undefined
            }
          })
        }
      }
    }
    
    const handlePointerUp = (e: MouseEvent | TouchEvent) => {
      e.preventDefault()
      const pos = getEventPos(e)
      
      if (isDraggingRef.current) {
        isDraggingRef.current = false
        container.style.cursor = 'grab'
      }
      
      if ('touches' in e) {
        // Handle touch end
        const touchDuration = Date.now() - touchStartTime
        
        if (touchDuration < 200 && !isDraggingRef.current) {
          // Single tap on mobile - directly open chart (no hover card)
          const fakeMouseEvent = {
            clientX: pos.x,
            clientY: pos.y,
            preventDefault: () => {}
          } as MouseEvent
          threeMouseMove(fakeMouseEvent)
          
          // Delay to let hover detection work, then open chart
          setTimeout(() => {
            console.log('Mobile tap detected, hovered company:', hoveredCompanyId)
            if (hoveredCompanyId) {
              dispatch({ type: 'charts/setActiveChart', payload: { ticker: hoveredCompanyId }})
              dispatch({ type: 'ui/openChartModal' })
            }
          }, 100)
        }
        lastTouchDistance = 0
      } else {
        // Handle mouse up
        if (isSelecting) {
          isSelecting = false
          performSelection(e)
        }
      }
    }
    
    const handleWheel = (e: WheelEvent) => {
      e.preventDefault()
      const delta = e.deltaY * -0.005
      handleZoom(delta, e.clientX, e.clientY)
    }
    
    const performSelection = (e: MouseEvent) => {
      if (!unprojectScreenCoords) return
    
      const startCoords = unprojectScreenCoords(selectionStart.x, selectionStart.y)
      const endCoords = unprojectScreenCoords(e.clientX, e.clientY)
      if(!startCoords || !endCoords) return;
    
      const getCoordsFn = getProjectedCoords;
      if (!getCoordsFn) return;
    
      switch (selectionMode) {
        case 'rect': {
          const min: Point2D = { x: Math.min(startCoords.x, endCoords.x), y: Math.min(startCoords.z, endCoords.z) }
          const max: Point2D = { x: Math.max(startCoords.x, endCoords.x), y: Math.max(startCoords.z, endCoords.z) }
          selectRectangle(min, max, getCoordsFn)
          break
        }
        case 'circle': {
          const center: Point2D = { x: (startCoords.x + endCoords.x) / 2, y: (startCoords.z + endCoords.z) / 2 }
          const radius = new THREE.Vector2(startCoords.x, startCoords.z).distanceTo(new THREE.Vector2(endCoords.x, endCoords.z)) / 2
          selectCircle(center, radius, getCoordsFn)
          break
        }
        case 'polygon': {
           if (selectionPoints.length >= 3) {
            const worldPoints = selectionPoints.map(p => unprojectScreenCoords(p.x, p.y)).filter(Boolean) as THREE.Vector3[]
            const projectedPoints: Point2D[] = worldPoints.map(p => ({ x: p.x, y: p.z }))
            selectPolygon(projectedPoints, getCoordsFn)
          }
          break
        }
      }
      
      dispatch({ type: 'map/setSelectionGeometry', payload: null })
      selectionPoints = []
    }
    
    // Mouse events
    container.addEventListener('mousedown', handlePointerDown)
    window.addEventListener('mousemove', handlePointerMove)
    window.addEventListener('mouseup', handlePointerUp)
    container.addEventListener('wheel', handleWheel, { passive: false })
    
    // Touch events
    container.addEventListener('touchstart', handlePointerDown, { passive: false })
    window.addEventListener('touchmove', handlePointerMove, { passive: false })
    window.addEventListener('touchend', handlePointerUp, { passive: false })
    
    // Prevent context menu on long press
    container.addEventListener('contextmenu', (e) => e.preventDefault())
    
    return () => {
      container.removeEventListener('mousedown', handlePointerDown)
      window.removeEventListener('mousemove', handlePointerMove)
      window.removeEventListener('mouseup', handlePointerUp)
      container.removeEventListener('wheel', handleWheel)
      
      container.removeEventListener('touchstart', handlePointerDown)
      window.removeEventListener('touchmove', handlePointerMove)
      window.removeEventListener('touchend', handlePointerUp)
      container.removeEventListener('contextmenu', (e) => e.preventDefault())
    }
  }, [selectionMode, handlePan, handleZoom, threeMouseMove, unprojectScreenCoords, getProjectedCoords, selectRectangle, selectCircle, selectPolygon, dispatch])

  const cursorStyle = useCallback(() => {
    switch (selectionMode) {
      case 'pan': return 'grab';
      case 'rect':
      case 'circle':
      case 'polygon':
      return 'crosshair';
      default: return 'default';
    }
  }, [selectionMode])
  
  return (
    <div
      ref={containerRef}
      className={`relative w-full h-full overflow-hidden ${className || ''}`}
      style={{
        cursor: cursorStyle(),
        willChange: performanceMode === 'high' ? 'transform' : 'auto',
        transform: 'translateZ(0)',
        touchAction: 'none', // Prevent default touch behaviors
        userSelect: 'none',   // Prevent text selection
        WebkitUserSelect: 'none',
        msUserSelect: 'none',
        WebkitTouchCallout: 'none' // Prevent iOS callout on long press
      }}
    >
      {!sceneReady && (
        <LoadingIndicator
          message="Initializing 3D map..."
          className="absolute inset-0 z-50 bg-gray-900"
        />
      )}
      <SelectionOverlay />
      <CompanyTooltip 
        mouseX={mousePos.x}
        mouseY={mousePos.y}
        visible={!!hoveredCompanyId}
      />
      {performanceMode === 'low' && (
        <div className="absolute bottom-4 right-4 bg-yellow-500/80 text-black px-2 py-1 rounded text-xs z-40">
          Performance Mode: Low
        </div>
      )}
    </div>
  )
}