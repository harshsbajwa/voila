/**
 * Custom error class for HTTP errors with status code
 */
export class HttpError extends Error {
  public readonly status: number
  public readonly statusText: string
  public readonly url?: string

  constructor(status: number, message?: string, url?: string) {
    super(message || `HTTP Error ${status}`)
    this.name = 'HttpError'
    this.status = status
    this.statusText = HttpError.getStatusText(status)
    this.url = url
  }

  static getStatusText(status: number): string {
    const statusTexts: Record<number, string> = {
      400: 'Bad Request',
      401: 'Unauthorized',
      403: 'Forbidden',
      404: 'Not Found',
      429: 'Too Many Requests',
      500: 'Internal Server Error',
      502: 'Bad Gateway',
      503: 'Service Unavailable',
      504: 'Gateway Timeout'
    }
    return statusTexts[status] || 'Unknown Error'
  }

  isRetryable(): boolean {
    // Retry on rate limiting, server errors, and network errors
    return this.status === 429 || this.status >= 500
  }

  isClientError(): boolean {
    return this.status >= 400 && this.status < 500
  }

  isServerError(): boolean {
    return this.status >= 500
  }
}

/**
 * Helper to create HttpError from fetch response
 */
export async function createHttpError(response: Response): Promise<HttpError> {
  let message = `HTTP ${response.status}: ${response.statusText}`
  
  try {
    const data = await response.json()
    if (data.message || data.detail || data.error) {
      message = data.message || data.detail || data.error
    }
  } catch {
    // Ignore JSON parse errors, use default message
  }
  
  return new HttpError(response.status, message, response.url)
}
