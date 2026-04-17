<template>
  <dv-border-box13>
    <div class="chart-container">
      <div ref="chartRef" class="chart"></div>
    </div>
  </dv-border-box13>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { merchantAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await merchantAPI.getDeliveryFeeRatingScatter()
    if (res.data.code === 200) {
      renderChart(res.data.data)
    } else {
      // 使用模拟数据
      renderMockData()
    }
  } catch (error) {
    console.error('加载配送费-评分散点图失败:', error)
    renderMockData()
  }
}

const renderMockData = () => {
  const mockData = [
    { deliveryFee: 0, rating: 4.8 },
    { deliveryFee: 0, rating: 4.6 },
    { deliveryFee: 0, rating: 4.9 },
    { deliveryFee: 0.5, rating: 4.7 },
    { deliveryFee: 0.5, rating: 4.5 },
    { deliveryFee: 0.5, rating: 4.8 },
    { deliveryFee: 1, rating: 4.6 },
    { deliveryFee: 1, rating: 4.4 },
    { deliveryFee: 1, rating: 4.7 },
    { deliveryFee: 1.5, rating: 4.5 },
    { deliveryFee: 1.5, rating: 4.3 },
    { deliveryFee: 1.5, rating: 4.6 },
    { deliveryFee: 2, rating: 4.4 },
    { deliveryFee: 2, rating: 4.2 },
    { deliveryFee: 2, rating: 4.5 },
    { deliveryFee: 2.5, rating: 4.3 },
    { deliveryFee: 2.5, rating: 4.1 },
    { deliveryFee: 2.5, rating: 4.4 },
    { deliveryFee: 3, rating: 4.2 },
    { deliveryFee: 3, rating: 4.0 },
    { deliveryFee: 3, rating: 4.3 },
    { deliveryFee: 4, rating: 4.1 },
    { deliveryFee: 4, rating: 3.9 },
    { deliveryFee: 4, rating: 4.2 },
    { deliveryFee: 5, rating: 4.0 },
    { deliveryFee: 5, rating: 3.8 },
    { deliveryFee: 5, rating: 4.1 },
  ]
  renderChart(mockData)
}

const renderChart = (data) => {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value)
  const scatterData = data.map((d) => [d.deliveryFee, d.rating])

  // 计算趋势线
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
      text: '配送费 vs 评分\n——————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'axis',
      formatter: (params) => {
        if (params[0].seriesName === '趋势线') return null
        return `配送费: ¥${params[0].value[0]}<br/>评分: ${params[0].value[1]}分`
      },
    },
    xAxis: {
      name: '配送费（元）',
      type: 'value',
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    yAxis: {
      name: '评分',
      min: 0,
      max: 5,
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    series: [
      {
        name: '店铺',
        type: 'scatter',
        data: scatterData,
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
