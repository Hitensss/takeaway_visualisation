<template>
  <dv-border-box-8>
    <div class="stats-card">
      <div class="card-header">
        <span class="card-title">📈 月售统计摘要</span>
      </div>
      <div class="stats-grid">
        <div v-for="item in data" :key="item.metricName" class="stat-item">
          <div class="stat-label">{{ item.metricName }}</div>
          <div class="stat-value">{{ formatValue(item.metricName, item.metricValue) }}</div>
        </div>
      </div>
    </div>
  </dv-border-box-8>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { merchantAPI } from '@/api'

const data = ref([])

const formatValue = (name, value) => {
  if (name === '平均月售' || name === '中位数月售') {
    return Math.round(value).toLocaleString() + '单'
  }
  if (name === '高销量商家占比') {
    return value + '%'
  }
  if (name.includes('分位数')) {
    return Math.round(value).toLocaleString() + '单'
  }
  return value.toLocaleString()
}

const loadData = async () => {
  try {
    const res = await merchantAPI.getSalesSummary()
    if (res.data.code === 200) {
      data.value = res.data.data
    }
  } catch (error) {
    console.error('加载月售统计摘要失败:', error)
  }
}

onMounted(() => {
  loadData()
})
</script>

<style scoped>
.stats-card {
  width: 100%;
  height: 450px;
  background: rgba(6, 30, 55, 0.4);
  border-radius: 12px;
  backdrop-filter: blur(5px);
  padding: 0;
  display: block inline;
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
  width: 100%;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(90px, 1fr));
  gap: 8px; /* 减小间距 */
  margin: 0; /* 去除外边距 */
  padding: 16px;
  transform: scale(0.9);
}
.stat-item {
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: flex-start;
  padding: 10px;
  background: rgba(0, 212, 255, 0.08);
  border-radius: 8px;
  border: 1px solid rgba(0, 212, 255, 0.15);
}
.stat-label {
  font-size: 12px;
  color: #bedbf8;
  margin-bottom: 6px;
}
.stat-value {
  font-size: 14px;
  font-weight: bold;
  color: #00d4ff;
}
</style>
