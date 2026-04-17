<template>
  <dv-border-box13>
    <div class="chart-container">
      <div class="stats-card">
        <div class="card-header">
          <span class="card-title">💰 起送价统计摘要</span>
        </div>
        <div class="stats-grid">
          <div v-for="item in data" :key="item.metricName" class="stat-item">
            <div class="stat-label">{{ item.metricName }}</div>
            <div class="stat-value">{{ formatValue(item.metricName, item.metricValue) }}</div>
          </div>
        </div>
      </div>
    </div>
  </dv-border-box13>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { merchantAPI } from '@/api'

const data = ref([])

const formatValue = (name, value) => {
  if (name === '免起送商家占比') {
    return value.toFixed(2) + '%'
  }
  return '¥' + value.toFixed(1)
}

const loadData = async () => {
  try {
    const res = await merchantAPI.getMinPriceSummary()
    if (res.data.code === 200) {
      data.value = res.data.data
    }
  } catch (error) {
    console.error('加载起送价统计摘要失败:', error)
  }
}

onMounted(() => {
  loadData()
})
</script>

<style scoped>
.chart-container {
  transform: scale(0.9);
  transform-origin: top center;
}

.stats-card {
  background: rgba(6, 30, 55, 0.4);
  border-radius: 12px;
  padding: 20px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
}
.card-header {
  margin-bottom: 16px;
  padding-bottom: 8px;
  border-bottom: 2px solid rgba(0, 212, 255, 0.3);
}
.card-title {
  font-size: 16px;
  font-weight: bold;
  color: #00d4ff;
}
.stats-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 12px;
}
.stat-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px;
  background: rgba(0, 212, 255, 0.3);
  border-radius: 8px;
}
.stat-label {
  font-size: 13px;
  color: #ffffff;
}
.stat-value {
  font-size: 16px;
  font-weight: bold;
  color: #00d4ff;
}
</style>
