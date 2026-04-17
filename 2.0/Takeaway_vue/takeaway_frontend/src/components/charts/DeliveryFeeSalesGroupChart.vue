<template>
  <dv-border-box13>
    <div class="chart-container">
      <div ref="chartRef" class="chart"></div>
      <div v-if="loading" class="loading">加载中...</div>
      <div v-if="error" class="error">{{ error }}</div>
    </div>
  </dv-border-box13>
</template>

<script setup>
import { ref, onMounted, onUnmounted, nextTick } from 'vue'
import * as echarts from 'echarts'
import { merchantAPI } from '@/api'

const chartRef = ref(null)
const loading = ref(true)
const error = ref('')
let chart = null

const loadData = async () => {
  loading.value = true
  error.value = ''
  try {
    const res = await merchantAPI.getDeliveryFeeSalesGroup()
    console.log('API响应:', res)

    if (res.data.code === 200) {
      const data = res.data.data
      console.log('图表数据:', data)

      if (!data || data.length === 0) {
        error.value = '暂无数据'
        return
      }

      await nextTick()
      renderChart(data)
    } else {
      error.value = res.data.message || '数据加载失败'
    }
  } catch (err) {
    console.error('加载失败:', err)
    error.value = '网络请求失败'
  } finally {
    loading.value = false
  }
}

const renderChart = (data) => {
  if (!chartRef.value) {
    console.error('chartRef 不存在')
    return
  }

  if (chart) {
    chart.dispose()
  }

  chart = echarts.init(chartRef.value)
  chart.setOption({
    title: {
      text: '不同配送费区间平均月售\n———————————',
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 'bold', color: '#00d4ff' },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params) => {
        const d = data[params[0].dataIndex]
        return `${d.feeGroup}<br/>平均月售: ${d.avgSales}单<br/>店铺数量: ${d.shopCount}家<br/>中位数月售: ${d.medianSales}单`
      },
    },
    xAxis: {
      name: '配送费区间',
      type: 'category',
      data: data.map((d) => d.feeGroup),
      axisLabel: { rotate: 30, interval: 0, color: '#00d4ff' },
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
          label: { show: true, position: 'top', formatter: '{c}单' },
        },
      },
    ],
  })

  // 调试：检查图表是否渲染
  console.log('图表渲染完成，实例:', chart)
}

const handleResize = () => {
  chart?.resize()
}

onMounted(async () => {
  await nextTick()
  await loadData()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  chart?.dispose()
})
</script>

<style scoped>
.chart-container {
  position: relative;
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
.loading,
.error {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  text-align: center;
}
.error {
  color: red;
}
</style>
