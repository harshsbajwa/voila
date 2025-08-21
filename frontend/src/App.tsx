import React, { Suspense, useEffect } from 'react'

interface LayoutShift extends PerformanceEntry {
  value: number
  hadRecentInput: boolean
}
import { Provider } from 'react-redux'
import { QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import { store } from '@/store'
import { queryClient } from '@/api/queryClient'
import { MarketDashboard } from '@/components/Dashboard/MarketDashboard'
import { LoadingIndicator } from '@/components/UI/LoadingIndicator'
import { trackFeatureUsage } from '@/store/middleware/analyticsMiddleware'
import { debug } from '@/utils/debug'

// Service Worker registration
const registerServiceWorker = async () => {
  if ('serviceWorker' in navigator && import.meta.env.PROD) {
    try {
      const registration = await navigator.serviceWorker.register('/sw.js', {
        scope: '/'
      })
      
      
      // Handle updates
      registration.addEventListener('updatefound', () => {
        const newWorker = registration.installing
        if (newWorker) {
          newWorker.addEventListener('statechange', () => {
            if (newWorker.state === 'installed' && navigator.serviceWorker.controller) {
              // New version available
              store.dispatch({
                type: 'ui/addNotification',
                payload: {
                  type: 'info' as const,
                  title: 'Update Available',
                  message: 'A new version is available. Refresh to update.',
                  duration: 0, // Persistent notification
                  actions: [{
                    label: 'Refresh',
                    action: () => window.location.reload()
                  }]
                }
              })
            }
          })
        }
      })
    } catch (error) {
      debug.error('Service Worker registration failed:', error)
    }
  }
}

// Performance monitoring
const initializePerformanceMonitoring = () => {
  // Monitor Core Web Vitals
  if ('PerformanceObserver' in window) {
    const observer = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (entry.entryType === 'largest-contentful-paint') {
          trackFeatureUsage('performance_lcp', { value: entry.startTime })
        } else if (entry.entryType === 'first-input') {
          trackFeatureUsage('performance_fid', { value: (entry as PerformanceEventTiming).processingStart - entry.startTime })
        } else if (entry.entryType === 'layout-shift') {
          if (!(entry as LayoutShift).hadRecentInput) {
            trackFeatureUsage('performance_cls', { value: (entry as LayoutShift).value })
          }
        }
      }
    })

    observer.observe({ entryTypes: ['largest-contentful-paint', 'first-input', 'layout-shift'] })
  }

  // Monitor memory usage
  if ('memory' in performance) {
    setInterval(() => {
      const memInfo = (performance as Performance & { memory: { usedJSHeapSize: number } }).memory
      store.dispatch({
        type: 'ui/updatePerformanceStats',
        payload: {
          memoryUsage: memInfo.usedJSHeapSize
        }
      })
    }, 10000) // Every 10 seconds
  }

  // Monitor network status
  const updateNetworkStatus = () => {
    store.dispatch({
      type: 'ui/addNotification',
      payload: {
        type: navigator.onLine ? 'success' : 'warning' as const,
        title: navigator.onLine ? 'Back Online' : 'Connection Lost',
        message: navigator.onLine 
          ? 'Real-time data is now available' 
          : 'Working offline with cached data',
        duration: 3000
      }
    })
  }

  window.addEventListener('online', updateNetworkStatus)
  window.addEventListener('offline', updateNetworkStatus)
}

// Error boundary for the entire app
class AppErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { hasError: boolean; error?: Error }
> {
  constructor(props: { children: React.ReactNode }) {
    super(props)
    this.state = { hasError: false }
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    debug.error('App Error Boundary:', error, errorInfo)
    
    // Track error
    trackFeatureUsage('app_error', {
      error: error.message,
      stack: error.stack,
      componentStack: errorInfo.componentStack
    })
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex items-center justify-center min-h-screen bg-gray-900 text-white">
          <div className="text-center max-w-md">
            <h1 className="text-3xl font-bold mb-4">Application Error</h1>
            <p className="text-gray-400 mb-6">
              Something unexpected happened. Please refresh the page to continue.
            </p>
            <div className="space-x-4">
              <button
                onClick={() => window.location.reload()}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors"
              >
                Refresh Page
              </button>
              <button
                onClick={() => this.setState({ hasError: false })}
                className="px-4 py-2 bg-gray-600 hover:bg-gray-700 rounded-lg transition-colors"
              >
                Try Again
              </button>
            </div>
            {import.meta.env.DEV && this.state.error && (
              <details className="mt-6 text-left text-xs">
                <summary className="cursor-pointer">Error Details</summary>
                <pre className="mt-2 p-4 bg-gray-800 rounded overflow-auto">
                  {this.state.error.stack}
                </pre>
              </details>
            )}
          </div>
        </div>
      )
    }

    return this.props.children
  }
}

// Main App component
const App: React.FC = () => {
  useEffect(() => {
    // Initialize application
    registerServiceWorker()
    initializePerformanceMonitoring()

    // Track app startup
    trackFeatureUsage('app_startup', {
      timestamp: Date.now(),
      userAgent: navigator.userAgent,
      viewport: {
        width: window.innerWidth,
        height: window.innerHeight
      }
    })

    // Detect mobile devices for responsive behavior
    const isMobile = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent)
    if (isMobile) {
      store.dispatch({ type: 'ui/setMobileLayout', payload: true })
    }

    // Handle app visibility changes (for performance optimization)
    const handleVisibilityChange = () => {
      if (document.hidden) {
        // App is hidden, reduce performance
        store.dispatch({ type: 'map/setPerformanceMode', payload: 'low' })
      } else {
        // App is visible, restore performance
        store.dispatch({ type: 'map/setPerformanceMode', payload: 'high' })
      }
    }

    document.addEventListener('visibilitychange', handleVisibilityChange)

    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange)
    }
  }, [])

  return (
    <AppErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <Provider store={store}>
          <div className="app">
            <Suspense 
              fallback={
                <div className="flex items-center justify-center min-h-screen bg-gray-900">
                  <LoadingIndicator 
                    size="lg" 
                    message="Initializing Market Dashboard..." 
                  />
                </div>
              }
            >
              <MarketDashboard />
            </Suspense>
          </div>
          
          {/* Development tools */}
          {import.meta.env.DEV && (
            <ReactQueryDevtools
              initialIsOpen={false}
            />
          )}
        </Provider>
      </QueryClientProvider>
    </AppErrorBoundary>
  )
}

export default App