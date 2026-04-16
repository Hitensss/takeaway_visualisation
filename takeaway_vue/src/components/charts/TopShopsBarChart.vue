<template>
  <div class="chart-container">
    <div ref="chartRef" class="chart"></div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { merchantAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await merchantAPI.getTopShops()
    if (res.data.code === 200) {
      const data = res.data.data.slice(0, 10).sort((a, b) => b.monthlySales - a.monthlySales)
      renderChart(data)
    }
  } catch (error) {
    console.error('加载Top10店铺失败:', error)
  }
}

const renderChart = (data) => {
  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: 'Top10 销量店铺',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold' },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params) => {
        const d = data[params[0].dataIndex]
        return `${d.shopName}<br/>月售: ${d.monthlySales}单<br/>品类: ${d.category}<br/>评分: ${d.rating}分<br/>人均: ¥${d.avgPrice}`
      },
    },
    grid: { left: '22%', right: '5%', containLabel: true },
    xAxis: { name: '月售（单）', type: 'value' },
    yAxis: {
      name: '店铺',
      type: 'category',
      data: data.map((d) =>
        d.shopName.length > 12 ? d.shopName.slice(0, 12) + '...' : d.shopName,
      ),
      axisLabel: { fontSize: 11 },
    },
    series: [
      {
        type: 'bar',
        data: data.map((d) => d.monthlySales),
        itemStyle: {
          color: '#e6a23c',
          borderRadius: [0, 4, 4, 0],
          label: { show: true, position: 'right', formatter: '{c}单' },
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
  height: 500px;
  background: white;
  border-radius: 12px;
  padding: 16px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
}
.chart {
  width: 100%;
  height: 100%;
}
</style>
