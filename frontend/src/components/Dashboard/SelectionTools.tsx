import React from 'react'
import { useSelection } from '@/hooks/useSelection' 
import type { SelectionMode } from '@/types/state'

const tools = [ 
  { id: 'pan', icon: '🖐️', title: 'Pan/Rotate' }, 
  { id: 'rect', icon: '⬜', title: 'Rectangle Select' }, 
  { id: 'circle', icon: '⭕', title: 'Circle Select' }, 
  { id: 'polygon', icon: '⬟', title: 'Polygon Select' }, 
] as const

export const SelectionTools: React.FC = () => { 
  const { selectionMode, setSelectionMode } = useSelection()

  const handleToolSelect = (mode: SelectionMode) => { setSelectionMode(mode) }

  return ( 
    <div className="tools"> 
      {tools.map((tool) => ( 
        <button key={tool.id} 
        className={`tool-btn ${selectionMode === tool.id ? 'active' : ''}`} 
        onClick={() => handleToolSelect(tool.id as SelectionMode)} 
        title={tool.title} > 
          <span className="tool-icon">{tool.icon}</span> 
        </button> 
      ))} 
    </div> 
  ) 
}