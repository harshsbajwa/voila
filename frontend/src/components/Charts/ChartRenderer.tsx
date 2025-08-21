import React, { useEffect, useRef } from 'react'
import * as d3 from 'd3'
import { useChartData } from '@/hooks/useMarketData'
import { LoadingIndicator } from '../UI/LoadingIndicator'
import type { OHLCVResponseRecord } from '@/types/api'

interface ChartRendererProps {
  ticker: string
}

type TooltipSelection = d3.Selection<HTMLDivElement, unknown, HTMLElement, any>;

const ChartRenderer: React.FC<ChartRendererProps> = ({ ticker }) => {
  const containerRef = useRef<HTMLDivElement>(null)
  const { data: chartData, isLoading, error } = useChartData(ticker, '1D', 90)

  useEffect(() => {
    if (!chartData || !containerRef.current || chartData.length === 0) return

    d3.select(containerRef.current).selectAll('*').remove()

    const tooltip = d3.select('body')
      .append('div')
      .attr('class', 'chart-tooltip')
      .style('position', 'absolute')
      .style('visibility', 'hidden')
      .style('background', 'rgba(0, 0, 0, 0.9)')
      .style('color', 'white')
      .style('padding', '10px')
      .style('border-radius', '5px')
      .style('font-size', '12px')
      .style('pointer-events', 'none')
      .style('z-index', '1000')

    createCandlestickChart(containerRef.current, chartData, tooltip)

    return () => {
      tooltip.remove()
    }
  }, [chartData])

  if (isLoading) {
    return <LoadingIndicator size="md" message="Loading chart data..." />
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full text-red-400">
        Failed to load chart data
      </div>
    )
  }

  if (!chartData || chartData.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400">
        No chart data available
      </div>
    )
  }

  return <div ref={containerRef} className="w-full h-full" />
}

function createCandlestickChart(container: HTMLElement, data: OHLCVResponseRecord[], tooltip: TooltipSelection) {
  const margin = { top: 20, right: 30, bottom: 40, left: 60 }
  const width = 760 - margin.left - margin.right
  const height = 400 - margin.top - margin.bottom

  const chartData = data.map(d => ({
    Date: new Date(d.Date),
    Open: d.Open,
    High: d.High,
    Low: d.Low,
    Close: d.Close,
    Volume: d.Volume
  })).sort((a, b) => a.Date.getTime() - b.Date.getTime())

  const xScale = d3.scaleTime()
    .domain(d3.extent(chartData, d => d.Date) as [Date, Date])
    .range([0, width])

  const yScale = d3.scaleLinear()
    .domain([
      (d3.min(chartData, d => d.Low) || 0) * 0.98,
      (d3.max(chartData, d => d.High) || 0) * 1.02
    ])
    .range([height, 0])

  const svg = d3.select(container)
    .append('svg')
    .attr('width', width + margin.left + margin.right)
    .attr('height', height + margin.top + margin.bottom)
    .style('background', 'transparent')

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`)

  const xAxis = d3.axisBottom(xScale)
    .tickFormat((domainValue) => d3.timeFormat('%m/%d')(domainValue as Date))

  const yAxis = d3.axisLeft(yScale)
    .tickFormat((domainValue) => `$${domainValue}`)

  g.append('g')
    .attr('transform', `translate(0,${height})`)
    .call(xAxis)
    .selectAll('text')
    .style('fill', '#9ca3af')

  g.append('g')
    .call(yAxis)
    .selectAll('text')
    .style('fill', '#9ca3af')

  const candleWidth = Math.min(8, width / chartData.length * 0.7)

  const candles = g.selectAll('.candle')
    .data(chartData)
    .enter().append('g')
    .attr('class', 'candle')

  candles.append('line')
    .attr('class', 'high-low-line')
    .attr('x1', d => xScale(d.Date))
    .attr('x2', d => xScale(d.Date))
    .attr('y1', d => yScale(d.High))
    .attr('y2', d => yScale(d.Low))
    .attr('stroke', d => d.Close >= d.Open ? '#10b981' : '#ef4444')
    .attr('stroke-width', 1)

  candles.append('rect')
    .attr('class', 'open-close-rect')
    .attr('x', d => xScale(d.Date) - candleWidth / 2)
    .attr('y', d => yScale(Math.max(d.Open, d.Close)))
    .attr('width', candleWidth)
    .attr('height', d => Math.abs(yScale(d.Open) - yScale(d.Close)) || 1)
    .attr('fill', d => d.Close >= d.Open ? '#10b981' : '#ef4444')
    .attr('fill-opacity', 0.8)

  candles
    .on('mouseover', function(_event, d) {
      tooltip.style('visibility', 'visible')
        .html(`
          <div>Date: ${d.Date.toLocaleDateString()}</div>
          <div>Open: $${d.Open.toFixed(2)}</div>
          <div>High: $${d.High.toFixed(2)}</div>
          <div>Low: $${d.Low.toFixed(2)}</div>
          <div>Close: $${d.Close.toFixed(2)}</div>
          <div>Volume: ${d.Volume.toLocaleString()}</div>
        `)
    })
    .on('mousemove', function(event) {
      tooltip
        .style('top', (event.pageY - 10) + 'px')
        .style('left', (event.pageX + 10) + 'px')
    })
    .on('mouseout', function() {
      tooltip.style('visibility', 'hidden')
    })
}

export default ChartRenderer