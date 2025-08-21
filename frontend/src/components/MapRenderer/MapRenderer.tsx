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
    
    const handleMouseDown = (e: MouseEvent) => {
      e.preventDefault()
      if (selectionMode === 'pan') {
        isDraggingRef.current = true
        container.style.cursor = 'grabbing'
      } else {
        isSelecting = true
        selectionStart = { x: e.clientX, y: e.clientY }
        selectionPoints = [{ x: e.clientX, y: e.clientY }]
      }
    }
    
    const handleMouseMove = (e: MouseEvent) => {
      e.preventDefault()
      setMousePos({ x: e.clientX, y: e.clientY })
      
      threeMouseMove(e)
      
      if (isDraggingRef.current) {
        handlePan(e.movementX, e.movementY)
      } else if (isSelecting) {
        if (selectionMode === 'polygon') {
            selectionPoints.push({ x: e.clientX, y: e.clientY })
        }
        dispatch({
          type: 'map/setSelectionGeometry',
          payload: {
            type: selectionMode,
            start: selectionStart,
            current: { x: e.clientX, y: e.clientY },
            points: selectionMode === 'polygon' ? selectionPoints : undefined
          }
        })
      }
    }
    
    const handleMouseUp = (e: MouseEvent) => {
      e.preventDefault()
      if (isDraggingRef.current) {
        isDraggingRef.current = false
        container.style.cursor = 'grab'
      }
      if (isSelecting) {
        isSelecting = false
        performSelection(e)
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
    
    container.addEventListener('mousedown', handleMouseDown)
    window.addEventListener('mousemove', handleMouseMove)
    window.addEventListener('mouseup', handleMouseUp)
    container.addEventListener('wheel', handleWheel, { passive: false })
    
    return () => {
      container.removeEventListener('mousedown', handleMouseDown)
      window.removeEventListener('mousemove', handleMouseMove)
      window.removeEventListener('mouseup', handleMouseUp)
      container.removeEventListener('wheel', handleWheel)
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