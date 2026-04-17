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
import { merchantAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await merchantAPI.getDeliveryTimeSalesGroup()
    if (res.data.code === 200) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载送达时间分组月售失败:', error)
  }
}

const renderChart = (data) => {
  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: '不同送达时间区间平均月售\n————————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params) => {
        const d = data[params[0].dataIndex]
        return `${d.timeGroup}<br/>平均月售: ${d.avgSales}单<br/>店铺数量: ${d.shopCount}家<br/>中位数月售: ${d.medianSales}单`
      },
    },
    xAxis: {
      name: '送达时间区间',
      type: 'category',
      data: data.map((d) => d.timeGroup),
      axisLabel: { rotate: 30, color: '#00d4ff' },
      nameTextStyle: { color: '#00d4ff' },
    },
    yAxis: {
      name: '平均月售（单）',
      type: 'value',
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    series: [
      {
        type: 'bar',
        data: data.map((d) => d.avgSales),
        itemStyle: {
          color: '#00d4ff',
          borderRadius: [4, 4, 0, 0],
        },
        label: {
          show: false,
          position: 'top',
          formatter: '{c}单',
          color: '#00d4ff',
          fontSize: 12,
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
  height: 102%;
}
</style>
