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
    if (res.data.code === 200) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载品类距离箱线图失败:', error)
  }
}

const renderChart = (data) => {
  chart = echarts.init(chartRef.value)
  const categories = data.map((d) => d.category)
  const boxplotData = data.map((d) => [
    d.minDistance,
    d.q1Distance,
    d.medianDistance,
    d.q3Distance,
    d.maxDistance,
  ])

  chart.setOption({
    title: {
      text: '各品类配送距离分布对比\n————————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params) => {
        const d = data[params[0].dataIndex]
        return `${d.category}<br/>最小值: ${d.minDistance}m<br/>下四分位: ${d.q1Distance}m<br/>中位数: ${d.medianDistance}m<br/>上四分位: ${d.q3Distance}m<br/>最大值: ${d.maxDistance}m<br/>平均值: ${d.avgDistance}m<br/>店铺数: ${d.shopCount}家`
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
      axisLabel: { color: '#00d4ff' },
      nameTextStyle: { color: '#00d4ff' },
    },
    series: [
      {
        name: '距离分布',
        type: 'boxplot',
        data: boxplotData,
        itemStyle: { color: '#00d4ff', borderColor: '#00d4ff' },
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
