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
    const res = await merchantAPI.getSalesDistribution()
    if (res.data.code === 200 && res.data.data && res.data.data.length > 0) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载月售分布失败:', error)
  }
}

const renderChart = (data) => {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: '月售分布\n————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    xAxis: {
      name: '月售区间',
      type: 'category',
      data: data.map((d) => d.salesBucket),
      axisLabel: { rotate: 30, fontSize: 11, color: '#00d4ff' },
      nameTextStyle: { color: '#00d4ff' },
    },
    yAxis: {
      name: '店铺数量',
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    series: [
      {
        type: 'bar',
        data: data.map((d) => d.shopCount),
        itemStyle: {
          color: '#00d4ff',
          borderRadius: [4, 4, 0, 0],
          label: { show: true, position: 'top' },
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
