<template>
  <dv-border-box-13>
    <div class="chart-container">
      <div ref="chartRef" class="chart"></div>
    </div>
  </dv-border-box-13>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { deliveryAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await deliveryAPI.getDistanceStats()
    if (res.data.code === 200) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载距离统计失败:', error)
  }
}

const renderChart = (data) => {
  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: '距离区间统计\n──────────',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      textStyle: { color: '#000000' },
      formatter: (params) => {
        const d = data[params[0].dataIndex]
        return `${d.distanceGroup}<br/>店铺数量: ${d.shopCount}家<br/>平均月售: ${d.avgSales}单<br/>平均评分: ${d.avgRating}分`
      },
    },
    xAxis: {
      name: '距离区间',
      type: 'category',
      data: data.map((d) => d.distanceGroup),
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
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
          label: { show: true, position: 'top', formatter: '{c}家', color: '#00d4ff' },
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
  box-shadow: 0 2px 12px rgba(17, 1, 1, 0.075);
}
.chart {
  width: 100%;
  height: 95%;
}
</style>
