import React, { useEffect } from 'react'
import { useSelector, useDispatch } from 'react-redux'
import type { RootState } from '@/types/state'

export const NotificationContainer: React.FC = () => {
  const dispatch = useDispatch()
  const notifications = useSelector((state: RootState) => state.ui.notifications)

  // Auto-remove notifications with duration
  useEffect(() => {
    const timers: Record<string, number> = {}

    notifications.forEach(notification => {
      if (notification.duration && notification.duration > 0) {
        timers[notification.id] = window.setTimeout(() => {
          dispatch({ type: 'ui/removeNotification', payload: notification.id })
        }, notification.duration)
      }
    })

    return () => {
      Object.values(timers).forEach(timer => window.clearTimeout(timer))
    }
  }, [notifications, dispatch])

  if (notifications.length === 0) return null

  return (
    <div className="fixed top-4 right-4 z-50 space-y-2 max-w-sm">
      {notifications.map(notification => (
        <div
          key={notification.id}
          className={`notification ${notification.type} animate-slide-in`}
        >
          <div className="flex justify-between items-start">
            <div className="flex-1">
              <div className="font-semibold text-sm">{notification.title}</div>
              <div className="text-xs text-gray-300 mt-1">{notification.message}</div>
            </div>
            <button
              onClick={() => dispatch({ type: 'ui/removeNotification', payload: notification.id })}
              className="ml-2 text-gray-400 hover:text-white text-sm"
            >
              ✕
            </button>
          </div>
          
          {notification.actions && (
            <div className="mt-2 flex space-x-2">
              {notification.actions.map((action, index) => (
                <button
                  key={index}
                  onClick={action.action}
                  className="text-xs px-2 py-1 bg-blue-600 hover:bg-blue-700 rounded transition-colors"
                >
                  {action.label}
                </button>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  )
}