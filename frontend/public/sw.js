// Service Worker for offline caching and performance optimization

const CACHE_NAME = 'market-dashboard-v1'
const RUNTIME_CACHE_NAME = 'market-dashboard-runtime-v1'

// Static assets to cache
const STATIC_ASSETS = [
  '/',
  '/index.html',
  '/manifest.json'
]

// API endpoints to cache with different strategies
const API_CACHE_STRATEGIES = {
  // Cache first for static reference data
  CACHE_FIRST: [
    '/api/v1/data/companies',
    '/api/v1/data/sectors',
    '/api/v1/data/states'
  ],
  
  // Network first for real-time data
  NETWORK_FIRST: [
    '/api/v1/market-data/overview',
    '/api/v1/market-data/ohlcv',
    '/api/v1/spatial'
  ],
  
  // Stale while revalidate for semi-real-time data
  STALE_WHILE_REVALIDATE: [
    '/api/v1/data/companies/',
    '/api/v1/data/complete/bulk'
  ]
}

// Install event - cache static assets
self.addEventListener('install', (event) => {
  console.log('Service Worker installing...')
  
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => {
        console.log('Caching static assets')
        return cache.addAll(STATIC_ASSETS)
      })
      .then(() => {
        // Skip waiting to activate immediately
        return self.skipWaiting()
      })
  )
})

// Activate event - cleanup old caches
self.addEventListener('activate', (event) => {
  console.log('Service Worker activating...')
  
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((cacheName) => {
          if (cacheName !== CACHE_NAME && cacheName !== RUNTIME_CACHE_NAME) {
            console.log('Deleting old cache:', cacheName)
            return caches.delete(cacheName)
          }
        })
      )
    }).then(() => {
      // Claim all clients immediately
      return self.clients.claim()
    })
  )
})

// Fetch event - implement caching strategies
self.addEventListener('fetch', (event) => {
  const { request } = event
  const url = new URL(request.url)
  
  // Skip non-GET requests
  if (request.method !== 'GET') {
    return
  }
  
  // Skip WebSocket requests
  if (url.protocol === 'ws:' || url.protocol === 'wss:') {
    return
  }

  // Handle API requests with different strategies
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(handleAPIRequest(request))
    return
  }

  // Handle static asset requests
  if (STATIC_ASSETS.some(asset => url.pathname.endsWith(asset))) {
    event.respondWith(handleStaticAsset(request))
    return
  }

  // Handle third-party resources (CDN, maps, etc.)
  if (url.origin !== self.location.origin) {
    event.respondWith(handleThirdPartyResource(request))
    return
  }

  // Default: network first for everything else
  event.respondWith(
    fetch(request).catch(() => {
      // Fallback to cache if network fails
      return caches.match(request)
    })
  )
})

// Handle API requests with appropriate caching strategy
async function handleAPIRequest(request) {
  const url = new URL(request.url)
  const pathname = url.pathname

  // Determine caching strategy based on endpoint
  let strategy = 'NETWORK_FIRST' // Default

  for (const [strategyName, patterns] of Object.entries(API_CACHE_STRATEGIES)) {
    if (patterns.some(pattern => pathname.startsWith(pattern))) {
      strategy = strategyName
      break
    }
  }

  switch (strategy) {
    case 'CACHE_FIRST':
      return cacheFirst(request)
    case 'NETWORK_FIRST':
      return networkFirst(request)
    case 'STALE_WHILE_REVALIDATE':
      return staleWhileRevalidate(request)
    default:
      return networkFirst(request)
  }
}

// Cache first strategy
async function cacheFirst(request) {
  const cachedResponse = await caches.match(request)
  
  if (cachedResponse) {
    // Optionally update cache in background
    updateCacheInBackground(request)
    return cachedResponse
  }
  
  // Fallback to network
  try {
    const networkResponse = await fetch(request)
    if (networkResponse.ok) {
      const cache = await caches.open(RUNTIME_CACHE_NAME)
      cache.put(request, networkResponse.clone())
    }
    return networkResponse
  } catch (error) {
    console.error('Cache first failed for:', request.url, error)
    throw error
  }
}

// Network first strategy
async function networkFirst(request) {
  try {
    const networkResponse = await fetch(request)
    
    if (networkResponse.ok) {
      // Cache successful responses
      const cache = await caches.open(RUNTIME_CACHE_NAME)
      
      // Only cache if response is not too large (prevent cache bloat)
      const contentLength = networkResponse.headers.get('content-length')
      if (!contentLength || parseInt(contentLength) < 1024 * 1024) { // 1MB limit
        cache.put(request, networkResponse.clone())
      }
    }
    
    return networkResponse
  } catch (error) {
    console.warn('Network request failed, trying cache:', request.url)
    
    // Fallback to cache
    const cachedResponse = await caches.match(request)
    if (cachedResponse) {
      return cachedResponse
    }
    
    // If we have no cache, return a custom offline response for API requests
    return createOfflineResponse(request)
  }
}

// Stale while revalidate strategy
async function staleWhileRevalidate(request) {
  const cache = await caches.open(RUNTIME_CACHE_NAME)
  const cachedResponse = await cache.match(request)
  
  // Always try to fetch from network
  const networkResponsePromise = fetch(request).then((networkResponse) => {
    if (networkResponse.ok) {
      cache.put(request, networkResponse.clone())
    }
    return networkResponse
  }).catch((error) => {
    console.warn('Network update failed for SWR:', request.url, error)
    return null
  })
  
  // Return cached response immediately if available
  if (cachedResponse) {
    return cachedResponse
  }
  
  // Otherwise wait for network response
  const networkResponse = await networkResponsePromise
  return networkResponse || createOfflineResponse(request)
}

// Handle static assets
async function handleStaticAsset(request) {
  const cachedResponse = await caches.match(request)
  
  if (cachedResponse) {
    return cachedResponse
  }
  
  try {
    const networkResponse = await fetch(request)
    if (networkResponse.ok) {
      const cache = await caches.open(CACHE_NAME)
      cache.put(request, networkResponse.clone())
    }
    return networkResponse
  } catch (error) {
    console.error('Failed to fetch static asset:', request.url, error)
    throw error
  }
}

// Handle third-party resources (CDN assets, map tiles, etc.)
async function handleThirdPartyResource(request) {
  const url = new URL(request.url)
  
  // Cache CDN assets aggressively
  if (url.hostname.includes('cdn.jsdelivr.net') || 
      url.hostname.includes('unpkg.com') ||
      url.hostname.includes('cdnjs.cloudflare.com')) {
    
    const cachedResponse = await caches.match(request)
    if (cachedResponse) {
      return cachedResponse
    }
    
    try {
      const networkResponse = await fetch(request)
      if (networkResponse.ok) {
        const cache = await caches.open(CACHE_NAME)
        cache.put(request, networkResponse.clone())
      }
      return networkResponse
    } catch (error) {
      console.warn('Third-party resource failed:', request.url)
      throw error
    }
  }
  
  // For other third-party resources, just try network
  return fetch(request)
}

// Update cache in background (for cache-first strategy)
function updateCacheInBackground(request) {
  fetch(request).then((networkResponse) => {
    if (networkResponse.ok) {
      caches.open(RUNTIME_CACHE_NAME).then((cache) => {
        cache.put(request, networkResponse.clone())
      })
    }
  }).catch((error) => {
    console.warn('Background cache update failed:', request.url, error)
  })
}

// Create offline response for API requests
function createOfflineResponse(request) {
  const url = new URL(request.url)
  
  // Return appropriate offline responses based on endpoint
  if (url.pathname.includes('/companies')) {
    return new Response(JSON.stringify([]), {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'X-Offline': 'true'
      }
    })
  }
  
  if (url.pathname.includes('/market-data/overview')) {
    return new Response(JSON.stringify({
      total_companies: 0,
      avg_price: 0,
      total_volume: 0,
      top_gainers: [],
      top_losers: [],
      offline: true
    }), {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'X-Offline': 'true'
      }
    })
  }
  
  // Generic offline response
  return new Response(JSON.stringify({
    error: 'Offline',
    message: 'This request requires an internet connection'
  }), {
    status: 503,
    headers: {
      'Content-Type': 'application/json',
      'X-Offline': 'true'
    }
  })
}

// Handle background sync (for when connection is restored)
self.addEventListener('sync', (event) => {
  console.log('Background sync triggered:', event.tag)
  
  if (event.tag === 'background-sync-market-data') {
    event.waitUntil(syncMarketData())
  }
})

// Sync market data when connection is restored
async function syncMarketData() {
  try {
    // Refresh critical market data
    const cache = await caches.open(RUNTIME_CACHE_NAME)
    
    const criticalEndpoints = [
      '/api/v1/market-data/overview',
      '/api/v1/data/companies?has_location=true&limit=1000'
    ]
    
    for (const endpoint of criticalEndpoints) {
      try {
        const response = await fetch(endpoint)
        if (response.ok) {
          await cache.put(endpoint, response.clone())
        }
      } catch (error) {
        console.warn('Failed to sync endpoint:', endpoint, error)
      }
    }
    
    console.log('Background sync completed')
  } catch (error) {
    console.error('Background sync failed:', error)
  }
}

// Handle push notifications (if needed for market alerts)
self.addEventListener('push', (event) => {
  if (!event.data) return
  
  const data = event.data.json()
  
  const options = {
    body: data.message,
    icon: '/icon-192.png',
    badge: '/badge-72.png',
    tag: data.tag || 'market-alert',
    requireInteraction: data.critical || false,
    data: data.url ? { url: data.url } : undefined
  }
  
  event.waitUntil(
    self.registration.showNotification(data.title || 'Market Alert', options)
  )
})

// Handle notification click
self.addEventListener('notificationclick', (event) => {
  event.notification.close()
  
  if (event.notification.data && event.notification.data.url) {
    event.waitUntil(
      clients.openWindow(event.notification.data.url)
    )
  }
})

// Periodic background sync for market data (if supported)
self.addEventListener('periodicsync', (event) => {
  if (event.tag === 'market-data-sync') {
    event.waitUntil(syncMarketData())
  }
})

// Message handling for communication with main thread
self.addEventListener('message', (event) => {
  const { type, payload } = event.data
  
  switch (type) {
    case 'SKIP_WAITING':
      self.skipWaiting()
      break
    case 'GET_CACHE_STATS':
      getCacheStats().then((stats) => {
        event.ports[0].postMessage(stats)
      })
      break
    case 'CLEAR_CACHE':
      clearAllCaches().then(() => {
        event.ports[0].postMessage({ success: true })
      })
      break
    default:
      console.warn('Unknown message type:', type)
  }
})

// Get cache statistics
async function getCacheStats() {
  const cacheNames = await caches.keys()
  let totalSize = 0
  let totalEntries = 0
  
  for (const cacheName of cacheNames) {
    const cache = await caches.open(cacheName)
    const requests = await cache.keys()
    totalEntries += requests.length
    
    // Estimate size (rough calculation)
    for (const request of requests) {
      const response = await cache.match(request)
      if (response) {
        const text = await response.text()
        totalSize += text.length
      }
    }
  }
  
  return {
    cacheNames,
    totalSize,
    totalEntries,
    estimatedSizeMB: Math.round(totalSize / 1024 / 1024 * 100) / 100
  }
}

// Clear all caches
async function clearAllCaches() {
  const cacheNames = await caches.keys()
  return Promise.all(
    cacheNames.map(cacheName => caches.delete(cacheName))
  )
}