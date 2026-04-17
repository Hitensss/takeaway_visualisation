<template>
  <dv-full-screen-container class="full-screen">
    <div class="bg"></div>
    <div class="big-screen">
      <!-- <dv-full-screen-container> -->

      <!-- 顶部标题 -->
      <div class="title">
        <div class="title-left">
          <decoration-1 :color="['#00d4ff', '#0066cc']" style="width: 200px; height: 60px" />
        </div>
        <div class="title-text">
          <h1>大学周边外卖餐饮数据分析系统</h1>
          <p>可视化大屏</p>
        </div>
        <div class="title-right">
          <decoration-1
            :color="['#00d4ff', '#0066cc']"
            :reverse="true"
            style="width: 200px; height: 60px"
          />
        </div>
      </div>

      <TopRightClock />
      <ShopCount />

      <!-- 按钮切换栏 -->
      <div class="tab-bar">
        <div
          v-for="tab in tabs"
          :key="tab.key"
          class="tab-btn"
          :class="{ active: activeTab === tab.key }"
          @click="activeTab = tab.key"
        >
          <decoration-5
            v-if="activeTab === tab.key"
            :color="['#00d4ff', '#0066cc']"
            style="width: 100%; height: 60px; position: absolute; top: 0; left: 0"
          />
          <span>{{ tab.name }}</span>
        </div>
      </div>

      <!-- 大屏内容区域 -->
      <div class="dashboard-container">
        <keep-alive>
          <component :is="currentComponent" />
        </keep-alive>
      </div>

      <!-- 底部装饰 -->
      <div class="footer">
        <decoration-2 :color="['#00d4ff', '#0066cc']" style="width: 500px; height: 30px" />
      </div>
      <!-- </dv-full-screen-container> -->
    </div>
  </dv-full-screen-container>
</template>

<script setup>
import { ref, computed } from 'vue'
import MerchantOverview from '@/views/dashboards/MerchantOverview.vue'
import DeliveryOverview from '@/views/dashboards/DeliveryOverview.vue'
import MerchantAnalysis from '@/views/dashboards/MerchantAnalysis.vue'
import DeliveryAnalysis from '@/views/dashboards/DeliveryAnalysis.vue'
import AdminPanel from '@/views/dashboards/AdminPanel.vue'
import TopRightClock from '@/components/TopRightClock.vue'
import ShopCount from '@/components/ShopCount.vue'
import { ca, cs } from 'element-plus/es/locale/index.mjs'

const activeTab = ref('merchant-overview')

const tabs = [
  { key: 'merchant-overview', name: '📊 外卖商家总览' },
  { key: 'delivery-overview', name: '🚚 配送总览' },
  { key: 'merchant-analysis', name: '📈 外卖商家分析' },
  { key: 'delivery-analysis', name: '📍 外卖配送分析' },
  { key: 'admin-panel', name: '⚙️ 后台数据管理' },
]

const currentComponent = computed(() => {
  switch (activeTab.value) {
    case 'merchant-overview':
      return MerchantOverview
    case 'delivery-overview':
      return DeliveryOverview
    case 'merchant-analysis':
      return MerchantAnalysis
    case 'delivery-analysis':
      return DeliveryAnalysis
    case 'admin-panel':
      return AdminPanel
    default:
      return MerchantOverview
  }
})
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

.full-screen {
  position: relative;
  width: 100%;
  min-height: 100vh;
  overflow: hidden;
}

.big-screen {
  position: relative;
  padding: 20px;
  box-sizing: border-box;
  background: transparent;
}

/*按钮组件*/
.tab-btn {
  position: relative;
  padding: 12px 32px;
  font-size: 16px;
  font-weight: bold;
  color: #a0c0e0;
  background: rgba(6, 30, 55, 0.6);
  border-radius: 30px;
  cursor: pointer;
  transition: all 0.3s;
  border: 1px solid rgba(0, 212, 255, 0.3);
  text-align: center;
  min-width: 150px;
}

.tab-btn:hover {
  color: #00d4ff;
  border-color: #00d4ff;
  background: rgba(0, 212, 255, 0.1);
}

.tab-btn.active {
  color: #00d4ff;
  border-color: #00d4ff;
  background: rgba(0, 212, 255, 0.15);
  box-shadow: 0 0 15px rgba(0, 212, 255, 0.3);
}

.tab-btn span {
  position: relative;
  z-index: 1;
}

.bg {
  position: absolute;
  inset: 0;
  background: url('@/assets/images/pageBg.png') no-repeat center center;
  background-size: cover;
  z-index: -1;
}

.title {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.title-text {
  text-align: center;
}

.title-text h1 {
  font-size: 28px;
  color: #00d4ff;
  text-shadow: 0 0 20px rgba(0, 212, 255, 0.5);
  letter-spacing: 4px;
}

.title-text p {
  font-size: 14px;
  color: rgba(0, 212, 255, 0.7);
  margin-top: 8px;
}

/* 按钮切换栏 */
.tab-bar {
  display: flex;
  justify-content: center;
  gap: 30px;
  margin-bottom: 30px;
  flex-wrap: wrap;
}

.dashboard-container {
  min-height: 600px;
}

.footer {
  text-align: center;
  margin-top: 30px;
  padding: 15px;
}

/* 通用卡片样式 */
.row {
  display: flex;
  gap: 20px;
  margin-bottom: 20px;
  flex-wrap: wrap;
}

.card {
  background: rgba(6, 30, 55, 0.4);
  border-radius: 12px;
  backdrop-filter: blur(5px);
  padding: 16px;
}

.card-title {
  font-size: 16px;
  font-weight: bold;
  color: #00d4ff;
  margin-bottom: 15px;
  padding-bottom: 8px;
  border-bottom: 1px solid rgba(0, 212, 255, 0.3);
}

.chart {
  width: 100%;
  height: 300px;
}

/* 统计卡片样式 */
.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 15px;
}

.stat-item {
  background: rgba(0, 212, 255, 0.1);
  border-radius: 8px;
  padding: 15px;
  text-align: center;
  border: 1px solid rgba(0, 212, 255, 0.2);
}

.stat-label {
  font-size: 14px;
  color: #a0c0e0;
  margin-bottom: 8px;
}

.stat-value {
  font-size: 24px;
  font-weight: bold;
  color: #00d4ff;
}
</style>
