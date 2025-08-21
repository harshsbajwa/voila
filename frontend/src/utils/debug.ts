const isDevelopment = import.meta.env.DEV
const isDebugEnabled = import.meta.env.VITE_ENABLE_DEBUG === 'true'

export const debug = {
  log: (...args: any[]) => {
    if (isDevelopment || isDebugEnabled) {
      console.log(...args)
    }
  },
  
  warn: (...args: any[]) => {
    if (isDevelopment || isDebugEnabled) {
      console.warn(...args)
    }
  },
  
  error: (...args: any[]) => {
    // Always log errors, but with less detail in production
    if (isDevelopment || isDebugEnabled) {
      console.error(...args)
    } else {
      console.error(args[0]) // Only log message in production
    }
  },
  
  info: (...args: any[]) => {
    if (isDevelopment || isDebugEnabled) {
      console.info(...args)
    }
  },
  
  time: (label: string) => {
    if (isDevelopment || isDebugEnabled) {
      console.time(label)
    }
  },
  
  timeEnd: (label: string) => {
    if (isDevelopment || isDebugEnabled) {
      console.timeEnd(label)
    }
  },
  
  group: (label: string) => {
    if (isDevelopment || isDebugEnabled) {
      console.group(label)
    }
  },
  
  groupEnd: () => {
    if (isDevelopment || isDebugEnabled) {
      console.groupEnd()
    }
  },
  
  table: (data: any) => {
    if (isDevelopment || isDebugEnabled) {
      console.table(data)
    }
  }
}

// Export individual functions for convenience
export const { log, warn, error, info, time, timeEnd, group, groupEnd, table } = debug
