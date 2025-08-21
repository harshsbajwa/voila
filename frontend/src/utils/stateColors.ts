// State adjacency map for proper graph coloring
// This ensures adjacent states don't have the same color

export const stateAdjacency: Record<string, string[]> = {
  'Alabama': ['Mississippi', 'Tennessee', 'Georgia', 'Florida'],
  'Alaska': [],
  'Arizona': ['California', 'Nevada', 'Utah', 'Colorado', 'New Mexico'],
  'Arkansas': ['Missouri', 'Tennessee', 'Mississippi', 'Louisiana', 'Texas', 'Oklahoma'],
  'California': ['Oregon', 'Nevada', 'Arizona'],
  'Colorado': ['Wyoming', 'Nebraska', 'Kansas', 'Oklahoma', 'New Mexico', 'Arizona', 'Utah'],
  'Connecticut': ['Massachusetts', 'Rhode Island', 'New York'],
  'Delaware': ['Maryland', 'Pennsylvania', 'New Jersey'],
  'Florida': ['Georgia', 'Alabama'],
  'Georgia': ['Florida', 'Alabama', 'Tennessee', 'North Carolina', 'South Carolina'],
  'Hawaii': [],
  'Idaho': ['Montana', 'Wyoming', 'Utah', 'Nevada', 'Oregon', 'Washington'],
  'Illinois': ['Wisconsin', 'Indiana', 'Kentucky', 'Missouri', 'Iowa'],
  'Indiana': ['Michigan', 'Ohio', 'Kentucky', 'Illinois'],
  'Iowa': ['Minnesota', 'Wisconsin', 'Illinois', 'Missouri', 'Nebraska', 'South Dakota'],
  'Kansas': ['Nebraska', 'Missouri', 'Oklahoma', 'Colorado'],
  'Kentucky': ['Indiana', 'Ohio', 'West Virginia', 'Virginia', 'Tennessee', 'Missouri', 'Illinois'],
  'Louisiana': ['Arkansas', 'Mississippi', 'Texas'],
  'Maine': ['New Hampshire'],
  'Maryland': ['Delaware', 'Pennsylvania', 'West Virginia', 'Virginia'],
  'Massachusetts': ['Rhode Island', 'Connecticut', 'New York', 'Vermont', 'New Hampshire'],
  'Michigan': ['Ohio', 'Indiana', 'Wisconsin'],
  'Minnesota': ['Wisconsin', 'Iowa', 'South Dakota', 'North Dakota'],
  'Mississippi': ['Louisiana', 'Arkansas', 'Tennessee', 'Alabama'],
  'Missouri': ['Iowa', 'Illinois', 'Kentucky', 'Tennessee', 'Arkansas', 'Oklahoma', 'Kansas', 'Nebraska'],
  'Montana': ['North Dakota', 'South Dakota', 'Wyoming', 'Idaho'],
  'Nebraska': ['South Dakota', 'Iowa', 'Missouri', 'Kansas', 'Colorado', 'Wyoming'],
  'Nevada': ['Idaho', 'Utah', 'Arizona', 'California', 'Oregon'],
  'New Hampshire': ['Maine', 'Massachusetts', 'Vermont'],
  'New Jersey': ['New York', 'Pennsylvania', 'Delaware'],
  'New Mexico': ['Colorado', 'Oklahoma', 'Texas', 'Arizona'],
  'New York': ['Vermont', 'Massachusetts', 'Connecticut', 'New Jersey', 'Pennsylvania'],
  'North Carolina': ['Virginia', 'Tennessee', 'Georgia', 'South Carolina'],
  'North Dakota': ['Minnesota', 'South Dakota', 'Montana'],
  'Ohio': ['Pennsylvania', 'West Virginia', 'Kentucky', 'Indiana', 'Michigan'],
  'Oklahoma': ['Kansas', 'Missouri', 'Arkansas', 'Texas', 'New Mexico', 'Colorado'],
  'Oregon': ['Washington', 'Idaho', 'Nevada', 'California'],
  'Pennsylvania': ['New York', 'New Jersey', 'Delaware', 'Maryland', 'West Virginia', 'Ohio'],
  'Rhode Island': ['Connecticut', 'Massachusetts'],
  'South Carolina': ['North Carolina', 'Georgia'],
  'South Dakota': ['North Dakota', 'Minnesota', 'Iowa', 'Nebraska', 'Wyoming', 'Montana'],
  'Tennessee': ['Kentucky', 'Virginia', 'North Carolina', 'Georgia', 'Alabama', 'Mississippi', 'Arkansas', 'Missouri'],
  'Texas': ['New Mexico', 'Oklahoma', 'Arkansas', 'Louisiana'],
  'Utah': ['Idaho', 'Wyoming', 'Colorado', 'Arizona', 'Nevada'],
  'Vermont': ['New York', 'New Hampshire', 'Massachusetts'],
  'Virginia': ['Maryland', 'West Virginia', 'Kentucky', 'Tennessee', 'North Carolina'],
  'Washington': ['Idaho', 'Oregon'],
  'West Virginia': ['Pennsylvania', 'Maryland', 'Virginia', 'Kentucky', 'Ohio'],
  'Wisconsin': ['Michigan', 'Minnesota', 'Iowa', 'Illinois'],
  'Wyoming': ['Montana', 'South Dakota', 'Nebraska', 'Colorado', 'Utah', 'Idaho']
}

// Color palette for states - using distinct colors
export const stateColorPalette = [
  0x3b82f6, // Blue
  0xef4444, // Red  
  0x10b981, // Green
  0xf59e0b, // Yellow
  0x8b5cf6, // Purple
  0x06b6d4, // Cyan
  0xf97316, // Orange
  0xec4899, // Pink
  0x6366f1, // Indigo
  0x84cc16, // Lime
]

// Map to store state color assignments
let stateColorMap: Record<string, number> = {}

/**
 * Assigns colors to states using graph coloring algorithm
 * Ensures no adjacent states have the same color
 */
export function assignStateColors(states: string[]): Record<string, number> {
  // Reset color map
  stateColorMap = {}
  
  // Sort states by number of neighbors (descending) for better coloring
  const sortedStates = [...states].sort((a, b) => {
    const aNeighbors = stateAdjacency[a]?.length || 0
    const bNeighbors = stateAdjacency[b]?.length || 0
    return bNeighbors - aNeighbors
  })
  
  // Assign colors using greedy algorithm
  sortedStates.forEach(state => {
    const neighbors = stateAdjacency[state] || []
    const usedColors = new Set<number>()
    
    // Find colors used by neighbors
    neighbors.forEach(neighbor => {
      if (stateColorMap[neighbor] !== undefined) {
        usedColors.add(stateColorMap[neighbor])
      }
    })
    
    // Find first available color
    let colorIndex = 0
    while (usedColors.has(colorIndex) && colorIndex < stateColorPalette.length) {
      colorIndex++
    }
    
    // Assign color (wrap around if needed)
    stateColorMap[state] = colorIndex % stateColorPalette.length
  })
  
  return stateColorMap
}

/**
 * Get color for a specific state
 */
export function getStateColor(stateName: string): number {
  if (stateColorMap[stateName] === undefined) {
    // Fallback for states not in the adjacency list
    const hash = stateName.split('').reduce((acc, char) => char.charCodeAt(0) + ((acc << 5) - acc), 0);
    const index = Math.abs(hash % stateColorPalette.length);
    return stateColorPalette[index];
  }
  
  return stateColorPalette[stateColorMap[stateName]]
}

/**
 * Get a highlighted version of the state color
 */
export function getHighlightedStateColor(stateName: string): number {
  const baseColor = getStateColor(stateName)
  // Create a brighter version by mixing with white
  const r = ((baseColor >> 16) & 0xff) * 0.7 + 255 * 0.3
  const g = ((baseColor >> 8) & 0xff) * 0.7 + 255 * 0.3
  const b = (baseColor & 0xff) * 0.7 + 255 * 0.3
  
  return (Math.floor(r) << 16) | (Math.floor(g) << 8) | Math.floor(b)
}