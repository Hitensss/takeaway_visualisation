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
    const res = await deliveryAPI.getFeeTimeGroup()
    if (res.data.code === 200 && res.data.data.length > 0) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载配送费分组送达时间失败:', error)
  }
}

const renderChart = (data) => {
  if (!chartRef.value) return

  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: '不同配送费区间平均送达时间\n—————————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params) => {
        const d = data[params[0].dataIndex]
        return `${d.feeGroup}<br/>平均送达时间: ${d.avgTime}分钟<br/>店铺数量: ${d.shopCount}家<br/>中位数: ${d.medianTime}分钟`
      },
    },
    xAxis: {
      name: '配送费区间',
      type: 'category',
      data: data.map((d) => d.feeGroup),
      axisLabel: { rotate: 30, color: '#00d4ff' },
      nameTextStyle: { color: '#00d4ff' },
    },
    yAxis: {
      name: '平均送达时间（分钟）',
      type: 'value',
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    series: [
      {
        type: 'bar',
        data: data.map((d) => d.avgTime),
        itemStyle: {
          color: '#00d4ff',
          borderRadius: [4, 4, 0, 0],
          label: { show: true, position: 'top', formatter: '{c}分钟' },
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
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
}
.chart {
  width: 100%;
  height: 100%;
}
</style>
