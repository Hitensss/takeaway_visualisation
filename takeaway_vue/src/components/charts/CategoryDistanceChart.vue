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
import { deliveryAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await deliveryAPI.getCategoryDistanceBoxplot()
    if (res.data.code === 200 && res.data.data.length > 0) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载品类距离分布失败:', error)
  }
}

const renderChart = (data) => {
  if (!chartRef.value) return

  const categories = data.map((d) => d.category)
  const distances = data.map((d) => d.medianDistance)

  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: '各品类配送距离对比\n——————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params) => {
        const d = data[params[0].dataIndex]
        return `${d.category}<br/>中位数距离: ${d.medianDistance}米<br/>平均距离: ${d.avgDistance}米<br/>店铺数量: ${d.shopCount}家`
      },
    },
    xAxis: {
      name: '品类',
      nameTextStyle: { color: '#00d4ff' },
      type: 'category',

      data: categories,
      axisLabel: { rotate: 45, fontSize: 11, color: '#00d4ff' },
    },
    yAxis: {
      name: '距离（米）',
      type: 'value',
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    series: [
      {
        type: 'bar',
        data: distances,
        itemStyle: {
          color: '#00d4ff',
          borderRadius: [4, 4, 0, 0],
          label: { show: true, position: 'top', formatter: '{c}m', color: '#00d4ff' },
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
  height: 360px;
  background: rgba(6, 30, 55, 0.4);
  border-radius: 12px;
  padding: 16px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
}
.chart {
  width: 100%;
  height: 95%;
}
</style>
