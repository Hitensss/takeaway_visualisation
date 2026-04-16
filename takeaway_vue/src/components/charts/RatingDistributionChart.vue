<template>
  <dv-border-box-8>
    <div class="chart-container">
      <div ref="chartRef" class="chart"></div>
    </div>
  </dv-border-box-8>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { merchantAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await merchantAPI.getRatingDistribution()
    if (res.data.code === 200 && res.data.data && res.data.data.length > 0) {
      renderChart(res.data.data)
    } else {
      console.warn('评分分布数据为空')
    }
  } catch (error) {
    console.error('加载评分分布失败:', error)
  }
}

const renderChart = (data) => {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: '评分分布\n————',
      left: 'right',
      textStyle: { fontSize: 20, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: { trigger: 'item', formatter: '{b}: {d}% ({c}家)' },
    legend: { orient: 'vertical', left: 'left', top: 'top', textStyle: { color: '#e0e0e0' } },
    series: [
      {
        type: 'pie',
        center: ['45%', '55%'],
        radius: ['35%', '65%'],
        label: { show: true, formatter: '{d}%', color: '#e0e0e0' },
        data: data.map((d) => ({ name: d.ratingBucket, value: d.shopCount })),
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
