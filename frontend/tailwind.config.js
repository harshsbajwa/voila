/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html", 
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        primary: '#00ff88',
        secondary: '#4488ff', 
        accent: '#ffaa00',
        danger: '#ff0044'
      },
      animation: {
        'spin': 'spin 0.8s linear infinite',
        'fade-in': 'fadeIn 0.3s ease-out',
        'slide-in': 'slideInFromRight 0.3s ease-out'
      }
    },
  },
  plugins: [],
}