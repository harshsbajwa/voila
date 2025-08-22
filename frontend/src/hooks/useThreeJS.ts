import { useEffect, useRef, useState, useCallback } from 'react'
import { useDispatch, useSelector } from 'react-redux'
import * as THREE from 'three'
import * as d3 from 'd3'
import { feature } from 'topojson-client'
import type { RootState } from '@/types/state'
import { assignStateColors, getStateColor } from '@/utils/stateColors'

const CONFIG = {
  PARTICLE_SIZE: 15,
  DEFAULT_COLOR: new THREE.Color(0x00ffff), // Cyan
  SELECTION_COLOR: new THREE.Color(0x00ff88), // Green
  HOVER_COLOR: new THREE.Color(0xffaa00), // Orange
  MAX_POINTS: 5000,
  MIN_ZOOM: 0.8,
  MAX_ZOOM: 250,
  // Camera bounds to keep view within reasonable limits
  MAX_PAN_DISTANCE: 2000, // Maximum distance camera can move from center
}

export const useThreeScene = (containerRef: React.RefObject<HTMLElement>) => {
  const dispatch = useDispatch()
  const { entities: companies, hoveredId } = useSelector((state: RootState) => state.companies)
  const { selectedCompanies } = useSelector((state: RootState) => state.selection)

  const rendererRef = useRef<THREE.WebGLRenderer>()
  const sceneRef = useRef<THREE.Scene>()
  const cameraRef = useRef<THREE.OrthographicCamera>()
  const pointsRef = useRef<THREE.Points>()
  const mapGroupRef = useRef<THREE.Group>()

  const stateRefs = useRef({
    indexToTicker: new Map<number, string>(),
    tickerToIndex: new Map<string, number>(),
    projectedCoords: [] as Array<{ ticker: string; x: number; y: number; z: number }>,
    projection: null as d3.GeoProjection | null,
    lastHoveredIndex: -1,
  }).current

  const [sceneReady, setSceneReady] = useState(false)

  const initScene = useCallback(() => {
    if (!containerRef.current || sceneRef.current) return

    const container = containerRef.current
    const width = container.clientWidth
    const height = container.clientHeight

    const scene = new THREE.Scene()
    scene.background = new THREE.Color(0x111827)
    sceneRef.current = scene

    const aspect = width / height
    const frustumSize = 1000
    const camera = new THREE.OrthographicCamera(
      frustumSize * aspect / -2, frustumSize * aspect / 2,
      frustumSize / 2, frustumSize / -2,
      0.1, 1000
    )
    camera.position.set(0, 50, 0)
    camera.lookAt(0, 0, 0)
    camera.updateProjectionMatrix()
    cameraRef.current = camera

    const renderer = new THREE.WebGLRenderer({ 
      antialias: window.devicePixelRatio <= 1, // Disable antialiasing on high DPI displays for performance
      powerPreference: 'high-performance',
      alpha: false,
      stencil: false,
      depth: true
    })
    renderer.setSize(width, height)
    // Limit pixel ratio to 2 for mobile performance
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2))
    rendererRef.current = renderer
    container.appendChild(renderer.domElement)

    const ambientLight = new THREE.AmbientLight(0xffffff, 1.5)
    scene.add(ambientLight)
    const dirLight = new THREE.DirectionalLight(0xffffff, 1.5)
    dirLight.position.set(5, 10, 7.5)
    scene.add(dirLight)

    mapGroupRef.current = new THREE.Group()
    scene.add(mapGroupRef.current)

    loadUSMap().then(() => {
      initPoints()
      setSceneReady(true)
    })
  }, [containerRef])

  const loadUSMap = useCallback(async () => {
    const us = await (await fetch('/us-states-10m.json')).json()
    const states = feature(us, us.objects.states)
    
    const projection = d3.geoAlbersUsa().scale(1000).translate([0, 0])
    stateRefs.projection = projection
    const stateFeatures = (states as unknown as GeoJSON.FeatureCollection).features
    assignStateColors(stateFeatures.map((f: GeoJSON.Feature) => (f.properties as { name: string }).name))

    stateFeatures.forEach((state: GeoJSON.Feature) => {
      if (!state.geometry) return
      const stateName = (state.properties as { name: string }).name
      const material = new THREE.MeshPhongMaterial({ color: getStateColor(stateName), transparent: true, opacity: 0.9 })
      const shapes = createShapesFromGeometry(state.geometry, projection)
      if (shapes.length > 0) {
        const geometry = new THREE.ExtrudeGeometry(shapes, { depth: 0.1, bevelEnabled: false })
        geometry.rotateX(-Math.PI / 2)
        const mesh = new THREE.Mesh(geometry, material)
        mesh.position.set(0, -0.1, 0)
        mesh.userData = { name: stateName, originalColor: material.color.getHex() }
        mapGroupRef.current?.add(mesh)
      }
    })
  }, [])

  const initPoints = useCallback(() => {
    if (!sceneRef.current) return

    const geometry = new THREE.BufferGeometry()
    const material = new THREE.PointsMaterial({
      size: CONFIG.PARTICLE_SIZE,
      vertexColors: true,
      sizeAttenuation: true,
      transparent: true,
      opacity: 0.9,
    })

    const points = new THREE.Points(geometry, material)
    pointsRef.current = points
    sceneRef.current.add(points)
  }, [])

  const updatePoints = useCallback(() => {
    const points = pointsRef.current
    const projection = stateRefs.projection
    if (!points || !projection) return

    const companiesArray = Object.values(companies)
    const validCompanies = companiesArray.filter(c => c.latitude && c.longitude)
    
    const positions = []
    const colors = []
    stateRefs.indexToTicker.clear()
    stateRefs.tickerToIndex.clear()
    stateRefs.projectedCoords = []

    let index = 0
    for (const company of validCompanies) {
      if (index >= CONFIG.MAX_POINTS) break
      
      const pos = projection([company.longitude!, company.latitude!])
      if (pos) {
        const [x, y] = pos
        const worldPos = new THREE.Vector3(x, 0.2, y)
        positions.push(worldPos.x, worldPos.y, worldPos.z)

        let color = CONFIG.DEFAULT_COLOR
        if (company.ticker === hoveredId) {
          color = CONFIG.HOVER_COLOR
        } else if (selectedCompanies.has(company.ticker)) {
          color = CONFIG.SELECTION_COLOR
        }
        colors.push(color.r, color.g, color.b)

        stateRefs.indexToTicker.set(index, company.ticker)
        stateRefs.tickerToIndex.set(company.ticker, index)
        stateRefs.projectedCoords.push({ ticker: company.ticker, x: worldPos.x, y: worldPos.y, z: worldPos.z })

        index++
      }
    }

    points.geometry.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3))
    points.geometry.setAttribute('color', new THREE.Float32BufferAttribute(colors, 3))
    points.geometry.attributes.position.needsUpdate = true
    points.geometry.attributes.color.needsUpdate = true
    points.geometry.computeBoundingSphere()

    dispatch({ type: 'ui/updatePerformanceStats', payload: { pointCount: index }})
  }, [companies, hoveredId, selectedCompanies, dispatch, stateRefs])
  
  useEffect(() => {
    if (sceneReady) {
      updatePoints()
    }
  }, [sceneReady, companies, hoveredId, selectedCompanies, updatePoints])

  const handlePan = useCallback((deltaX: number, deltaY: number) => {
    const camera = cameraRef.current
    if (!camera) return
    
    const panSpeed = 2.5 / camera.zoom
    const newX = camera.position.x - deltaX * panSpeed
    const newZ = camera.position.z - deltaY * panSpeed
    
    // Apply bounds to prevent camera from going too far
    const maxDistance = CONFIG.MAX_PAN_DISTANCE
    const distanceFromCenter = Math.sqrt(newX * newX + newZ * newZ)
    
    if (distanceFromCenter <= maxDistance) {
      camera.position.x = newX
      camera.position.z = newZ
    } else {
      // Scale down the movement to stay within bounds
      const scale = maxDistance / distanceFromCenter
      camera.position.x = newX * scale
      camera.position.z = newZ * scale
    }
  }, [])
  
  const handleZoom = useCallback((delta: number, mouseX: number, mouseY: number) => {
    const camera = cameraRef.current
    const container = containerRef.current
    if (!camera || !container) return

    const worldPosBefore = unprojectScreenCoords(mouseX, mouseY)
    const newZoom = THREE.MathUtils.clamp(camera.zoom + delta * camera.zoom * 0.5, CONFIG.MIN_ZOOM, CONFIG.MAX_ZOOM)
    camera.zoom = newZoom
    camera.updateProjectionMatrix()
    const worldPosAfter = unprojectScreenCoords(mouseX, mouseY)
    if (worldPosBefore && worldPosAfter) {
      const dx = worldPosAfter.x - worldPosBefore.x
      const dz = worldPosAfter.z - worldPosBefore.z
      camera.position.x += dx
      camera.position.z += dz
    }
  }, [])

  const handleMouseMove = useCallback((event: MouseEvent) => {
    const camera = cameraRef.current
    const container = containerRef.current
    const points = pointsRef.current
    if (!camera || !container || !points || !points.geometry.attributes.position) return

    const mouse = new THREE.Vector2(
      (event.clientX / container.clientWidth) * 2 - 1,
      -(event.clientY / container.clientHeight) * 2 + 1
    )
    
    const raycaster = new THREE.Raycaster()
    raycaster.setFromCamera(mouse, camera)
    raycaster.params.Points.threshold = CONFIG.PARTICLE_SIZE / camera.zoom;
    
    const intersects = raycaster.intersectObject(points)
    
    if (intersects.length > 0 && intersects[0].index !== undefined) {
      const index = intersects[0].index
      if (stateRefs.lastHoveredIndex !== index) {
        const ticker = stateRefs.indexToTicker.get(index)
        if (ticker) {
          dispatch({ type: 'companies/setHoveredCompany', payload: ticker })
          stateRefs.lastHoveredIndex = index
        }
      }
    } else if (stateRefs.lastHoveredIndex !== -1) {
      dispatch({ type: 'companies/setHoveredCompany', payload: null })
      stateRefs.lastHoveredIndex = -1
    }
  }, [dispatch, stateRefs])
  
  const handleClick = useCallback(() => {
    if (hoveredId) {
      dispatch({ type: 'charts/setActiveChart', payload: { ticker: hoveredId }})
      dispatch({ type: 'ui/openChartModal' })
    }
  }, [hoveredId, dispatch])

  const animate = useCallback(() => {
    const renderer = rendererRef.current
    const scene = sceneRef.current
    const camera = cameraRef.current
    const points = pointsRef.current

    if (!renderer || !scene || !camera || !points) return

    if (points.material instanceof THREE.PointsMaterial) {
      const newSize = CONFIG.PARTICLE_SIZE / Math.pow(camera.zoom, 0.05);
      points.material.size = THREE.MathUtils.clamp(newSize, 1, 30);
    }

    renderer.render(scene, camera)
    const info = renderer.info.render
    dispatch({type: 'ui/updatePerformanceStats', payload: { drawCalls: info.calls, renderTime: performance.now() }})
  }, [dispatch])

  const unprojectScreenCoords = useCallback((screenX: number, screenY: number): THREE.Vector3 | null => {
    const camera = cameraRef.current
    const container = containerRef.current
    if (!camera || !container) return null
    const vec = new THREE.Vector3(
      (screenX / container.clientWidth) * 2 - 1,
      -(screenY / container.clientHeight) * 2 + 1,
      0.5
    )
    vec.unproject(camera)
    const dir = vec.sub(camera.position).normalize()
    const distance = -camera.position.y / dir.y
    return camera.position.clone().add(dir.multiplyScalar(distance))
  }, [])

  const getProjectedCoords = useCallback(() => stateRefs.projectedCoords, [stateRefs])

  useEffect(() => {
    initScene()
    const container = containerRef.current
    return () => {
      if (rendererRef.current) {
        container?.removeChild(rendererRef.current.domElement)
        rendererRef.current.dispose()
        sceneRef.current = undefined
      }
    }
  }, [initScene])

  useEffect(() => {
    const handleResize = () => {
      const container = containerRef.current
      const camera = cameraRef.current
      const renderer = rendererRef.current
      if (container && camera && renderer) {
        const width = container.clientWidth
        const height = container.clientHeight
        const aspect = width / height
        camera.left = - ( (camera.top - camera.bottom) * aspect ) / 2
        camera.right = ( (camera.top - camera.bottom) * aspect ) / 2
        camera.updateProjectionMatrix()
        renderer.setSize(width, height)
      }
    }
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  useEffect(() => {
    const canvas = rendererRef.current?.domElement
    if (!canvas) return
    canvas.addEventListener('click', handleClick)
    return () => canvas.removeEventListener('click', handleClick)
  }, [handleClick])

  return {
    sceneReady,
    animate,
    handlePan,
    handleZoom,
    handleMouseMove,
    unprojectScreenCoords,
    getProjectedCoords
  }
}

function createShapesFromGeometry(geometry: any, projection: d3.GeoProjection): THREE.Shape[] {
    const shapes: THREE.Shape[] = [];
    const createShape = (polygon: number[][][]) => {
        const shape = new THREE.Shape();
        const outerRing = polygon[0];
        const projectedOuter = outerRing.map(p => projection(p as [number, number])).filter(Boolean) as [number, number][];
        if (projectedOuter.length > 0) {
            shape.moveTo(projectedOuter[0][0], -projectedOuter[0][1]);
            for (let i = 1; i < projectedOuter.length; i++) {
                shape.lineTo(projectedOuter[i][0], -projectedOuter[i][1]);
            }
        }
        for (let i = 1; i < polygon.length; i++) {
            const holeRing = polygon[i];
            const projectedHole = holeRing.map(p => projection(p as [number, number])).filter(Boolean) as [number, number][];
            if (projectedHole.length > 0) {
                const holePath = new THREE.Path();
                holePath.moveTo(projectedHole[0][0], -projectedHole[0][1]);
                for (let j = 1; j < projectedHole.length; j++) {
                    holePath.lineTo(projectedHole[j][0], -projectedHole[j][1]);
                }
                shape.holes.push(holePath);
            }
        }
        return shape;
    };
    if (geometry.type === 'Polygon') {
        shapes.push(createShape(geometry.coordinates));
    } else if (geometry.type === 'MultiPolygon') {
        geometry.coordinates.forEach((polygon: number[][][]) => {
            shapes.push(createShape(polygon));
        });
    }
    return shapes;
}