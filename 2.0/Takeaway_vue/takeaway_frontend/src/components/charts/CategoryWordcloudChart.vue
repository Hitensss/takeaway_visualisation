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
import 'echarts-wordcloud'
import { merchantAPI } from '@/api'

const chartRef = ref(null)
let chart = null

const loadData = async () => {
  try {
    const res = await merchantAPI.getCategoryWordcloud()
    console.log('词云数据:', res.data)
    if (res.data.code === 200 && res.data.data && res.data.data.length > 0) {
      renderChart(res.data.data)
    } else {
      console.warn('词云数据为空')
    }
  } catch (error) {
    console.error('加载品类词云失败:', error)
  }
}

const renderChart = (data) => {
  if (!chartRef.value) return

  // 转换数据格式，只使用 category 和 weight
  const chartData = data.map((item) => ({
    name: item.category,
    value: item.weight,
    // 保留原始数据用于tooltip
    category: item.category,
    weight: item.weight,
  }))

  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: '品类评分词云（评分越高字越大）\n———————————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'item',
      formatter: (params) => {
        return `${params.name}<br/>权重: ${params.data.weight}`
      },
    },
    series: [
      {
        type: 'wordCloud',
        shape: 'circle',
        width: '100%',
        height: '85%',
        sizeRange: [14, 60],
        rotationRange: [-45, 90],
        rotationStep: 45,
        gridSize: 8,
        drawOutOfBound: false,
        textStyle: {
          fontFamily: 'sans-serif',
          fontWeight: 'bold',
          color: function () {
            return (
              'rgb(' +
              [
                Math.floor(Math.random() * 160) + 40,
                Math.floor(Math.random() * 160) + 40,
                Math.floor(Math.random() * 160) + 40,
              ].join(',') +
              ')'
            )
          },
        },
        data: chartData,
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
