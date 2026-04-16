<template>
  <div class="chart-container">
    <div ref="chartRef" class="chart"></div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { merchantAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await merchantAPI.getSalesDeliveryTimeScatter()
    if (res.data.code === 200) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载月售-送达时间散点图失败:', error)
  }
}

const renderChart = (data) => {
  chart = echarts.init(chartRef.value)
  const scatterData = data.map((d) => [d.deliveryTime, d.monthlySales])

  const n = scatterData.length
  let sumX = 0,
    sumY = 0,
    sumXY = 0,
    sumX2 = 0
  scatterData.forEach((p) => {
    sumX += p[0]
    sumY += p[1]
    sumXY += p[0] * p[1]
    sumX2 += p[0] * p[0]
  })
  const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX)
  const intercept = (sumY - slope * sumX) / n
  const xMin = Math.min(...scatterData.map((p) => p[0]))
  const xMax = Math.max(...scatterData.map((p) => p[0]))
  const trendData = [
    [xMin, slope * xMin + intercept],
    [xMax, slope * xMax + intercept],
  ]

  chart.setOption({
    title: {
      text: '月售 vs 送达时间',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold' },
    },
    tooltip: {
      trigger: 'axis',
      formatter: (params) => {
        if (params[0].seriesName === '趋势线') return null
        return `送达时间: ${params[0].value[0]}分钟<br/>月售: ${params[0].value[1]}单`
      },
    },
    xAxis: { name: '送达时间（分钟）', type: 'value' },
    yAxis: { name: '月售（单）', type: 'value' },
    series: [
      {
        name: '店铺',
        type: 'scatter',
        data: scatterData,
        symbolSize: 8,
        itemStyle: { color: '#5470c6', opacity: 0.6 },
      },
      {
        name: '趋势线',
        type: 'line',
        data: trendData,
        lineStyle: { color: '#ff6600', width: 2, type: 'dashed' },
        symbol: 'none',
        tooltip: { show: false },
      },
    ],
  })
}

const handleResize = () => chart?.resize()
onMounted(() => {
  loadData()
  window.addEventListener('resize', handleResize)
})
onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  chart?.dispose()
})
</script>

<style scoped>
.chart-container {
  width: 100%;
  height: 450px;
  background: white;
  border-radius: 12px;
  padding: 16px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
}
.chart {
  width: 100%;
  height: 100%;
}
</style>
