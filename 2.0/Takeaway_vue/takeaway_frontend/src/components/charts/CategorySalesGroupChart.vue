<template>
  <dv-border-box8 :reverse="true">
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
    const res = await merchantAPI.getCategorySalesDistribution()
    if (res.data.code === 200) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载品类月售分布失败:', error)
  }
}

const renderChart = (data) => {
  chart = echarts.init(chartRef.value)
  const categories = [...new Set(data.map((d) => d.category))]
  const salesBuckets = [...new Set(data.map((d) => d.salesBucket))]
  const seriesData = salesBuckets.map((bucket) => ({
    name: bucket,
    type: 'bar',
    stack: 'total',
    data: categories.map((cat) => {
      const item = data.find((d) => d.category === cat && d.salesBucket === bucket)
      return item ? item.shopCount : 0
    }),
  }))

  chart.setOption({
    title: {
      text: '各品类月售分布\n——————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { data: salesBuckets, top: 30, textStyle: { color: '#a0c0e0' } },
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
      itemStyle: { color: ['#5470c6', '#67c23a', '#e6a23c', '#f56c6c', '#909399'][i % 5] },
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
