<template>
  <dv-border-box1>
    <div class="chart-container">
      <div ref="chartRef" class="chart"></div>
    </div>
  </dv-border-box1>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { deliveryAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await deliveryAPI.getCorrelationMatrix()
    if (res.data.code === 200) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载相关性矩阵失败:', error)
  }
}

const renderChart = (data) => {
  chart = echarts.init(chartRef.value)
  const variables = [...new Set(data.map((d) => d.variable1))]
  const heatData = []
  for (const item of data) {
    const xIdx = variables.indexOf(item.variable1)
    const yIdx = variables.indexOf(item.variable2)
    heatData.push([xIdx, yIdx, item.correlation])
    if (item.variable1 !== item.variable2) heatData.push([yIdx, xIdx, item.correlation])
  }

  chart.setOption({
    title: {
      text: '变量相关性热力图\n————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'item',
      formatter: (params) =>
        `${variables[params.data[0]]} ↔ ${variables[params.data[1]]}<br/>相关系数: ${params.data[2].toFixed(3)}`,
    },
    xAxis: {
      type: 'category',
      data: variables,
      axisLabel: { rotate: 45, fontSize: 11, color: '#00d4ff' },
      nameTextStyle: { color: '#00d4ff' },
    },
    yAxis: {
      type: 'category',
      data: variables,
      axisLabel: { fontSize: 11, color: '#00d4ff' },
      nameTextStyle: { color: '#00d4ff' },
    },
    visualMap: {
      min: -1,
      max: 1,
      calculable: true,
      orient: 'vertical',
      right: 10,
      top: 'center',
      inRange: {
        color: [
          '#313695',
          '#4575b4',
          '#74add1',
          '#abd9e9',
          '#e0f3f8',
          '#ffffbf',
          '#fee090',
          '#fdae61',
          '#f46d43',
          '#d73027',
          '#a50026',
        ],
      },
    },
    series: [
      {
        type: 'heatmap',
        data: heatData,
        label: { show: true, formatter: (p) => p.data[2].toFixed(2), fontSize: 10 },
        emphasis: { scale: false },
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
