<template>
  <dv-border-box8>
    <div class="chart-title">
      <decoration-10 :color="['#00d4ff', '#0066cc']" style="width: 30px; height: 30px" />
      <span>各品类平均月售排名</span>
      <decoration-10
        :color="['#00d4ff', '#0066cc']"
        :reverse="true"
        style="width: 30px; height: 30px"
      />
    </div>
    <div ref="chartRef" class="chart"></div>
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
    const res = await merchantAPI.getCategoryStats()
    if (res.data.code === 200) {
      const data = res.data.data.sort((a, b) => b.avgSales - a.avgSales)
      renderChart(data)
    }
  } catch (error) {
    console.error('加载品类统计失败:', error)
  }
}

const renderChart = (data) => {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value)
  chart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: '12%', right: '8%', containLabel: true, borderWidth: 0 },
    xAxis: {
      name: '平均月售（单）',
      type: 'value',
      axisLabel: { color: '#a0c0e0', fontSize: 11 },
      axisLine: { lineStyle: { color: '#00d4ff' } },
      splitLine: { lineStyle: { color: 'rgba(0, 212, 255, 0.1)' } },
    },
    yAxis: {
      name: '品类',
      type: 'category',
      data: data.map((d) => d.category),
      axisLabel: { color: '#00d4ff', fontSize: 12, fontWeight: 'bold' },
      axisLine: { lineStyle: { color: '#00d4ff' } },
    },
    series: [
      {
        type: 'bar',
        data: data.map((d) => d.avgSales),
        itemStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
            { offset: 0, color: '#00d4ff' },
            { offset: 1, color: '#0066cc' },
          ]),
          borderRadius: [0, 4, 4, 0],
          label: { show: true, position: 'right', color: '#00d4ff', formatter: '{c}单' },
        },
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
}

.chart-title {
  display: flex;
  justify-content: flex-start;
  align-items: center;
  gap: 15px;
  margin: 0;
  padding: 0 16px 16px 0;
  width: 100%;

  font-size: 16px;
  font-weight: bold;
  color: #00d4ff;
  letter-spacing: 2px;
}

.chart {
  width: 100%;
  height: calc(100% - 50px);
}
</style>
