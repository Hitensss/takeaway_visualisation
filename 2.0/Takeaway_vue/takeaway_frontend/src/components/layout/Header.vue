<template>
  <div class="header">
    <div class="header-title">
      <h1>🏪 大学周边外卖餐饮数据分析系统</h1>
    </div>
    <div class="header-stats">
      <div class="stat-item">
        <span class="stat-label">数据更新时间</span>
        <span class="stat-value">{{ dataTime }}</span>
      </div>
      <div class="stat-item">
        <span class="stat-label">店铺总数</span>
        <span class="stat-value">{{ shopCount }}</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { merchantAPI } from '@/api'

const dataTime = ref('')
const shopCount = ref('--')

const getCurrentDate = () => {
  const now = new Date()
  dataTime.value = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
}

const loadShopCount = async () => {
  try {
    const res = await merchantAPI.getSalesSummary()
    if (res.data.code === 200) {
      const totalShops = res.data.data.find((item) => item.metricName === '店铺总数')
      if (totalShops) {
        shopCount.value = totalShops.metricValue.toFixed(0)
      }
    }
  } catch (error) {
    console.error('加载店铺总数失败:', error)
    shopCount.value = '--'
  }
}

onMounted(() => {
  getCurrentDate()
  loadShopCount()
})
</script>

<style scoped>
.header {
  background: white;
  padding: 0 24px;
  height: 64px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.08);
  position: sticky;
  top: 0;
  z-index: 10;
}

.header-title h1 {
  font-size: 20px;
  color: #1a1a2e;
  margin: 0;
}

.header-stats {
  display: flex;
  gap: 24px;
}

.stat-item {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
}

.stat-label {
  font-size: 12px;
  color: #999;
}

.stat-value {
  font-size: 16px;
  font-weight: bold;
  color: #1a1a2e;
}
</style>
