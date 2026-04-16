<template>
  <dv-border-box12>
    <div class="chart-container">
      <div ref="chartRef" class="chart"></div>
    </div>
  </dv-border-box12>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { deliveryAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await deliveryAPI.getFeeDistanceScatter()
    if (res.data.code === 200) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载配送费-距离散点图失败:', error)
  }
}

const renderChart = (data) => {
  chart = echarts.init(chartRef.value)
  const feeData = data.map((d) => [d.distance, d.deliveryFee])

  // 计算趋势线
  const n = feeData.length
  let sumX = 0,
    sumY = 0,
    sumXY = 0,
    sumX2 = 0
  feeData.forEach((p) => {
    sumX += p[0]
    sumY += p[1]
    sumXY += p[0] * p[1]
    sumX2 += p[0] * p[0]
  })
  const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX)
  const intercept = (sumY - slope * sumX) / n
  const xMin = Math.min(...feeData.map((p) => p[0]))
  const xMax = Math.max(...feeData.map((p) => p[0]))
  const trendData = [
    [xMin, slope * xMin + intercept],
    [xMax, slope * xMax + intercept],
  ]

  chart.setOption({
    title: {
      text: '配送费 vs 距离\n————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'axis',
      formatter: (params) => {
        if (params[0].seriesName === '趋势线') return null
        return `距离: ${params[0].value[0]}米<br/>配送费: ¥${params[0].value[1]}`
      },
    },
    xAxis: {
      name: '距离（米）',
      type: 'value',
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    yAxis: {
      name: '配送费（元）',
      type: 'value',
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    series: [
      {
        name: '店铺',
        type: 'scatter',
        data: feeData,
        symbolSize: 8,
        itemStyle: { color: '#00d4ff', opacity: 0.6 },
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
  height: 400px;
  background: rgba(6, 30, 55, 0.4);
  border-radius: 12px;
  padding: 16px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
}
.chart {
  width: 100%;
  height: 100%;
}
</style>
