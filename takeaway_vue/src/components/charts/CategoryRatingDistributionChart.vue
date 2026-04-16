<template>
  <dv-border-box8>
    <div class="chart-container">
      <div ref="chartRef" class="chart"></div>
    </div>
  </dv-border-box8>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { merchantAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await merchantAPI.getCategoryRatingDistribution()
    if (res.data.code === 200 && res.data.data.length > 0) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载品类评分分布失败:', error)
  }
}

const renderChart = (data) => {
  if (!chartRef.value) return

  const categories = [...new Set(data.map((d) => d.category))]
  const ratingBuckets = [...new Set(data.map((d) => d.ratingBucket))]

  const seriesData = ratingBuckets.map((bucket) => ({
    name: bucket,
    type: 'bar',
    stack: 'total',
    data: categories.map((cat) => {
      const item = data.find((d) => d.category === cat && d.ratingBucket === bucket)
      return item ? item.shopCount : 0
    }),
  }))

  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: '各品类评分分布\n——————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, textStyle: { color: '#ffffff' } },
    legend: {
      data: ratingBuckets,
      top: 35,
      right: '5%',
      orient: 'vertical',
      textStyle: { color: '#a0c0e0' },
    },
    grid: { top: 80 },
    xAxis: {
      name: '品类',
      type: 'category',
      data: categories,
      axisLabel: { rotate: 45, fontSize: 11, color: '#00d4ff' },
      nameTextStyle: { color: '#00d4ff' },
    },
    yAxis: {
      name: '店铺数量',
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    series: seriesData.map((s, i) => ({
      ...s,
      itemStyle: { color: ['#a0d6f0', '#4ecb73', '#ffb347', '#ff6b6b', '#00d4ff'][i % 5] },
      borderRadius: [4, 4, 0, 0],
    })),
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
