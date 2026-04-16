<template>
  <div class="sidebar">
    <div class="logo">
      <dv-decoration-5 :width="200" :height="80" />
      <div class="logo-text">
        <span class="logo-icon">🍔</span>
        <span class="logo-title">外卖数据分析</span>
      </div>
    </div>
    <div class="menu">
      <el-menu
        :default-active="activeMenu"
        class="sidebar-menu"
        @select="handleMenuSelect"
        background-color="transparent"
        text-color="#a0c0e0"
        active-text-color="#00d4ff"
      >
        <el-sub-menu index="merchant">
          <template #title>
            <span>📊 商家分析</span>
          </template>
          <el-menu-item index="merchant-overview">
            <span>📋 商家总览</span>
          </el-menu-item>
          <el-menu-item index="merchant-analysis">
            <span>📈 商家分析</span>
          </el-menu-item>
        </el-sub-menu>

        <el-menu-item index="delivery">
          <span>🚚 外卖配送分析</span>
        </el-menu-item>
      </el-menu>
    </div>
    <div class="sidebar-footer">
      <dv-decoration-9 :width="200" :height="40" />
    </div>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'

const router = useRouter()
const route = useRoute()
const activeMenu = ref('merchant-overview')

watch(
  () => route.path,
  (newPath) => {
    if (newPath.includes('/merchant/overview')) activeMenu.value = 'merchant-overview'
    else if (newPath.includes('/merchant/analysis')) activeMenu.value = 'merchant-analysis'
    else if (newPath.includes('/delivery')) activeMenu.value = 'delivery'
  },
  { immediate: true },
)

const handleMenuSelect = (index) => {
  if (index === 'merchant-overview') router.push({ name: 'MerchantOverview' })
  else if (index === 'merchant-analysis') router.push({ name: 'MerchantAnalysis' })
  else if (index === 'delivery') router.push({ name: 'DeliveryAnalysis' })
}
</script>

<style scoped>
.sidebar {
  width: 240px;
  height: 100vh;
  background: linear-gradient(180deg, #0a1628 0%, #061a2f 100%);
  position: fixed;
  left: 0;
  top: 0;
  overflow-y: auto;
  border-right: 1px solid rgba(0, 212, 255, 0.2);
  z-index: 100;
  display: flex;
  flex-direction: column;
}

.logo {
  text-align: center;
  padding: 20px 0;
  position: relative;
}

.logo-text {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  width: 100%;
  text-align: center;
}

.logo-icon {
  font-size: 32px;
  display: block;
  margin-bottom: 8px;
}

.logo-title {
  font-size: 16px;
  font-weight: bold;
  color: #00d4ff;
  letter-spacing: 2px;
}

.menu {
  flex: 1;
  padding: 20px 0;
}

.sidebar-menu {
  border: none;
  background: transparent;
}

.sidebar-menu .el-menu-item,
.sidebar-menu .el-sub-menu .el-sub-menu__title {
  height: 50px;
  line-height: 50px;
  margin: 4px 12px;
  border-radius: 8px;
  color: #a0c0e0;
}

.sidebar-menu .el-menu-item:hover,
.sidebar-menu .el-sub-menu .el-sub-menu__title:hover {
  background: rgba(0, 212, 255, 0.1);
  color: #00d4ff;
}

.sidebar-menu .el-menu-item.is-active {
  background: rgba(0, 212, 255, 0.15);
  color: #00d4ff;
  box-shadow: 0 0 10px rgba(0, 212, 255, 0.3);
}

:deep(.el-sub-menu .el-menu) {
  background: transparent;
}

:deep(.el-sub-menu .el-menu-item) {
  padding-left: 48px !important;
  margin: 2px 12px;
}

.sidebar-footer {
  padding: 20px 0;
  text-align: center;
}

/* 滚动条样式 */
.sidebar::-webkit-scrollbar {
  width: 4px;
}

.sidebar::-webkit-scrollbar-track {
  background: #0a1628;
}

.sidebar::-webkit-scrollbar-thumb {
  background: #00d4ff;
  border-radius: 2px;
}
</style>
