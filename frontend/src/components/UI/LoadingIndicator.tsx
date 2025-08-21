import React from 'react'

interface LoadingIndicatorProps {
  message?: string
  className?: string
  size?: 'sm' | 'md' | 'lg'
}

export const LoadingIndicator: React.FC<LoadingIndicatorProps> = ({ 
  message = 'Loading...', 
  className = '',
  size = 'md'
}) => {
  const sizeClasses = {
    sm: 'w-8 h-8',
    md: 'w-12 h-12', 
    lg: 'w-16 h-16'
  }

  const spinnerSize = sizeClasses[size]

  return (
    <div className={`flex items-center justify-center ${className}`}>
      <div className="text-center">
        <div 
          className={`${spinnerSize} border-3 border-gray-600 border-t-green-400 rounded-full animate-spin mx-auto`}
        />
        {message && (
          <p className="mt-4 text-gray-400 text-sm">
            {message}
          </p>
        )}
      </div>
    </div>
  )
}