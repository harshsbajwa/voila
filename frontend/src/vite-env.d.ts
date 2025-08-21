/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_URL: string
  readonly VITE_ENABLE_DEBUG: string
  readonly VITE_ENABLE_TEST_LOADER: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}