<template>
  <dv-border-box-8>
    <div class="stats-card">
      <div class="card-header">
        <span class="card-title">📊 品类评分统计</span>
      </div>
      <div class="stats-grid">
        <div v-for="item in data" :key="item.category" class="stat-item">
          <div class="stat-category">{{ item.category }}</div>
          <div class="stat-values">
            <span class="stat-badge">⭐ {{ item.avgRating }}分</span>
            <span class="stat-badge">🏪 {{ item.shopCount }}家</span>
            <span class="stat-badge">📈 {{ item.highRatingRatio }}%高评</span>
          </div>
        </div>
      </div>
    </div>
  </dv-border-box-8>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { merchantAPI } from '@/api'

const data = ref([])

const getColor = (rating) => {
  if (rating >= 4.6) return '#67c23a'
  if (rating >= 4.4) return '#85ce61'
  if (rating >= 4.2) return '#b3d9a0'
  if (rating >= 4.0) return '#e6a23c'
  return '#f56c6c'
}

const loadData = async () => {
  try {
    const res = await merchantAPI.getCategoryRatingStats()
    if (res.data.code === 200) {
      data.value = res.data.data
    }
  } catch (error) {
    console.error('加载品类评分统计失败:', error)
  }
}

onMounted(() => {
  loadData()
})
</script>

<style scoped>
.stats-card {
  background: rgba(6, 30, 55, 0.4);
  /*border-radius: 8px;*/
  backdrop-filter: blur(5px);
  overflow: hidden;
  box-shadow:
    0 0 8px rgba(0, 212, 255, 0.3),
    inset 0 0 8px rgba(0, 212, 255, 0.05);
  text-align: center;
  /*border: 1px solid rgba(0, 212, 255, 0.2);*/
  border: none;
  padding: 12px;
  display: block;
  margin: 0 auto;
  width: min(100%, 720px);
  height: 420px;
}

.card-title {
  font-size: 16px;
  font-weight: bold;
  color: #00d4ff;
  padding: 16px 16px 0 16px;
  margin-bottom: 8px;
}

.stat-item {
  display: flex;
  flex-direction: column;
  gap: 0px;
  align-items: stretch;
  background: rgba(0, 212, 255, 0.08);
  border-radius: 8px;
  padding: 10px;
  border: 1px solid rgba(0, 212, 255, 0.18);
  box-shadow: 0 0 10px rgba(0, 212, 255, 0.22);
  transform: scale(0.9);
}

.stat-values {
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  gap: 5px;
}

.stat-label {
  font-size: 11px;
  color: #a0c0e0;
  margin-bottom: 4px;
}

/* 品类名称  */
.stat-category {
  font-size: 13px;
  font-weight: bold;
  color: #e0e0e0; /* 亮灰色 */
  margin-bottom: 6px;
  text-align: center;
}

/* 徽章文字颜色 */
.stat-badge {
  font-size: 10px;
  color: #c0d4f0; /* 更亮的蓝灰色 */
  background: rgba(0, 212, 255, 0.12);
  padding: 2px 6px;
  border-radius: 10px;
  white-space: nowrap;
}

.stat-value {
  font-size: 18px;
  font-weight: bold;
  color: #00d4ff;
}

.stats-grid {
  width: 100%;
  height: 400px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(70px, 1fr));
  gap: 10px;
  padding: 12px;
  border: none;
  background: transparent;
}

/*  滚动条
.stats-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 10px;
  padding: 16px;
  max-height: 400px;
  overflow-y: auto;
}

.stats-grid::-webkit-scrollbar {
  width: 4px;
}

.stats-grid::-webkit-scrollbar-track {
  background: rgba(0, 212, 255, 0.1);
  border-radius: 2px;
}

.stats-grid::-webkit-scrollbar-thumb {
  background: rgba(0, 212, 255, 0.4);
  border-radius: 2px;
}
*/
</style>
