import type { Middleware } from '@reduxjs/toolkit'
import type { RootState } from '@/types/state'

// Analytics event tracking
interface AnalyticsEvent {
  type: string
  payload: any
  timestamp: number
  userId?: string
  sessionId: string
}

let sessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
let analyticsQueue: AnalyticsEvent[] = []

export const analyticsMiddleware: Middleware<{}, RootState> = (_store) => {
  // Flush analytics queue periodically
  const flushInterval = setInterval(() => {
    if (analyticsQueue.length > 0) {
      flushAnalytics()
    }
  }, 30000) // Every 30 seconds

  // Flush on page unload
  window.addEventListener('beforeunload', () => {
    flushAnalytics()
    clearInterval(flushInterval)
  })

  return (next) => (action: any) => {
    const startTime = performance.now()
    const result = next(action)
    const endTime = performance.now()
    
    // Track performance-sensitive actions
    trackActionPerformance(action.type, endTime - startTime)
    
    // Track specific user interactions
    if (shouldTrackAction(action.type)) {
      trackAnalyticsEvent({
        type: action.type,
        payload: sanitizePayload(action.payload),
        timestamp: Date.now(),
        sessionId
      })
    }
    
    // Track errors
    if (action.type.endsWith('/setError') && action.payload) {
      trackError(action.type, action.payload)
    }
    
    return result
  }
}

// Determine which actions to track
const shouldTrackAction = (actionType: string): boolean => {
  const trackedActions = [
    // User interactions
    'companies/setFilters',
    'selection/setSelectedCompanies',
    'selection/setSelectionMode',
    'charts/setActiveChart',
    'map/setSelectionMode',
    'map/focusOnRegion',
    'ui/setTheme',
    
    // Performance events
    'ui/updatePerformanceStats',
    
    // Feature usage
    'charts/addTechnicalIndicator',
    'selection/addToSelectionHistory'
  ]
  
  return trackedActions.some(pattern => 
    actionType.includes(pattern.split('/')[0]) && 
    actionType.includes(pattern.split('/')[1])
  )
}

// Sanitize payload to remove sensitive data
const sanitizePayload = (payload: any): any => {
  if (!payload) return payload
  
  if (typeof payload === 'object') {
    // Handle non-serializable objects like Sets
    if (payload instanceof Set) {
        return { type: 'Set', size: payload.size, values: Array.from(payload).slice(0, 10) }
    }
    
    const sanitized: any = {}
    
    for (const [key, value] of Object.entries(payload)) {
      // Skip sensitive fields
      if (key.toLowerCase().includes('password') || 
          key.toLowerCase().includes('token') ||
          key.toLowerCase().includes('secret')) {
        continue
      }
      
      // Truncate long strings
      if (typeof value === 'string' && value.length > 100) {
        sanitized[key] = value.substring(0, 100) + '...'
      } else if (typeof value === 'object') {
        sanitized[key] = sanitizePayload(value)
      } else {
        sanitized[key] = value
      }
    }
    
    return sanitized
  }
  
  return payload
}

// Track analytics event
const trackAnalyticsEvent = (event: AnalyticsEvent) => {
  analyticsQueue.push(event)
  
  // Flush immediately if queue is getting large
  if (analyticsQueue.length >= 50) {
    flushAnalytics()
  }
}

// Track action performance
const trackActionPerformance = (actionType: string, duration: number) => {
  // Only track slow actions
  if (duration > 10) { // More than 10ms
    trackAnalyticsEvent({
      type: 'performance',
      payload: {
        action: actionType,
        duration: Math.round(duration * 100) / 100, // Round to 2 decimal places
        category: 'action_performance'
      },
      timestamp: Date.now(),
      sessionId
    })
  }
}

// Track errors
const trackError = (actionType: string, error: any) => {
  trackAnalyticsEvent({
    type: 'error',
    payload: {
      source: actionType,
      message: typeof error === 'string' ? error : error?.message || 'Unknown error',
      stack: error?.stack,
      category: 'application_error'
    },
    timestamp: Date.now(),
    sessionId
  })
}

// Flush analytics to server
const flushAnalytics = async () => {
  if (analyticsQueue.length === 0) return
  
  const events = [...analyticsQueue]
  analyticsQueue = []
  
  try {
    if (import.meta.env.DEV) {
      return;
    }
    
    // Production analytics endpoint
    await fetch('/api/analytics/events', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        events,
        metadata: {
          userAgent: navigator.userAgent,
          viewport: {
            width: window.innerWidth,
            height: window.innerHeight
          },
          timestamp: Date.now()
        }
      })
    })
  } catch (error) {
    // Add events back to queue for retry
    analyticsQueue.unshift(...events)
  }
}

// User session tracking
export const trackUserSession = () => {
  const sessionStart = Date.now()
  
  trackAnalyticsEvent({
    type: 'session_start',
    payload: {
      userAgent: navigator.userAgent,
      viewport: {
        width: window.innerWidth,
        height: window.innerHeight
      },
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      language: navigator.language
    },
    timestamp: sessionStart,
    sessionId
  })
  
  // Track session duration on page unload
  window.addEventListener('beforeunload', () => {
    trackAnalyticsEvent({
      type: 'session_end',
      payload: {
        duration: Date.now() - sessionStart,
        category: 'user_engagement'
      },
      timestamp: Date.now(),
      sessionId
    })
  })
}

// Feature usage tracking
export const trackFeatureUsage = (feature: string, details?: any) => {
  trackAnalyticsEvent({
    type: 'feature_usage',
    payload: {
      feature,
      details: sanitizePayload(details),
      category: 'feature_interaction'
    },
    timestamp: Date.now(),
    sessionId
  })
}

// Performance metrics tracking
export const trackPerformanceMetric = (metric: string, value: number, unit?: string) => {
  trackAnalyticsEvent({
    type: 'performance_metric',
    payload: {
      metric,
      value,
      unit: unit || 'ms',
      category: 'performance'
    },
    timestamp: Date.now(),
    sessionId
  })
}

// Custom event tracking
export const trackCustomEvent = (eventName: string, properties?: any) => {
  trackAnalyticsEvent({
    type: 'custom_event',
    payload: {
      event: eventName,
      properties: sanitizePayload(properties),
      category: 'custom'
    },
    timestamp: Date.now(),
    sessionId
  })
}

// Initialize session tracking
trackUserSession()