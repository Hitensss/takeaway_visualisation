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
import { merchantAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await merchantAPI.getDistanceRatingBoxplot()
    if (res.data.code === 200) {
      renderChart(res.data.data)
    }
  } catch (error) {
    console.error('加载距离-评分箱线图失败:', error)
  }
}

const renderChart = (data) => {
  chart = echarts.init(chartRef.value)
  const boxplotData = data.map((d) => [
    d.minRating,
    d.q1Rating,
    d.medianRating,
    d.q3Rating,
    d.maxRating,
  ])

  chart.setOption({
    title: {
      text: '不同距离区间评分分布对比\n————————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params) => {
        const d = data[params[0].dataIndex]
        return `${d.distanceGroup}<br/>最小值: ${d.minRating}分<br/>下四分位: ${d.q1Rating}分<br/>中位数: ${d.medianRating}分<br/>上四分位: ${d.q3Rating}分<br/>最大值: ${d.maxRating}分<br/>平均值: ${d.meanRating}分<br/>店铺数: ${d.shopCount}家`
      },
    },
    xAxis: {
      name: '距离区间',
      type: 'category',
      data: data.map((d) => d.distanceGroup),
      axisLabel: { fontSize: 12, color: '#00d4ff' },
      nameTextStyle: { color: '#00d4ff' },
    },
    yAxis: {
      name: '评分',
      min: 0,
      max: 5,
      nameTextStyle: { color: '#00d4ff' },
      axisLabel: { color: '#00d4ff' },
    },
    series: [
      {
        name: '评分分布',
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
