<template>
  <div class="shop-count-wrapper">
    <border-box-8 :color="['#00d4ff', '#0066cc']" style="width: 100%; height: 100%">
      <div class="shop-count-content">
        <div class="shop-icon">🏪</div>
        <div class="shop-info">
          <div class="shop-label">入驻商家总数</div>
          <div class="shop-value">
            <dv-digital-flop :config="digitalConfig" style="width: 120px; height: 40px" />
          </div>
        </div>
      </div>
    </border-box-8>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { merchantAPI } from '@/api'

const digitalConfig = ref({
  number: [0],
  content: '{nt}',
  textAlign: 'center',
  style: { fontSize: 28, fontWeight: 'bold', fill: '#00d4ff' },
})

const loadShopCount = async () => {
  try {
    const res = await merchantAPI.getCategoryStats()
    if (res.data.code === 200) {
      const total = res.data.data.reduce((sum, item) => sum + item.shopCount, 0)
      digitalConfig.value.number = [total]
    }
  } catch (error) {
    console.error('加载店铺总数失败:', error)
    digitalConfig.value.number = [258]
  }
}

onMounted(() => {
  loadShopCount()
})
</script>

<style scoped>
.shop-count-wrapper {
  position: absolute;
  top: 20px;
  left: 20px;
  width: 240px;
  height: 100px;
  z-index: 10;
}
.shop-count-content {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 0 15px;
  gap: 12px;
}
.shop-icon {
  font-size: 40px;
  filter: drop-shadow(0 0 8px rgba(0, 212, 255, 0.6));
}
.shop-info {
  flex: 1;
}
.shop-label {
  font-size: 12px;
  color: #a0c0e0;
  margin-bottom: 4px;
}
.shop-value {
  font-size: 28px;
  font-weight: bold;
  color: #00d4ff;
  line-height: 1;
  margin-bottom: 4px;
}
</style>
