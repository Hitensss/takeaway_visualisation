<template>
  <div class="top-right-clock">
    <div class="clock-box">
      <span class="clock-label">当前时间</span>
      <span class="clock-value">{{ timeText }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'

const timeText = ref('')
let timer = null

const updateTime = () => {
  const now = new Date()
  const date = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(
    now.getDate(),
  ).padStart(2, '0')}`
  const time = now.toLocaleTimeString('zh-CN', { hour12: false })
  timeText.value = `${date} ${time}`
}

onMounted(() => {
  updateTime()
  timer = setInterval(updateTime, 1000)
})

onUnmounted(() => {
  clearInterval(timer)
})
</script>

<style scoped>
.top-right-clock {
  position: absolute;
  top: 20px;
  right: 20px;
  z-index: 20;
}

.clock-box {
  min-width: 180px;
  padding: 10px 16px;
  border-radius: 18px;
  background: rgba(0, 18, 44, 0.85);
  border: 1px solid rgba(0, 212, 255, 0.3);
  color: #ffffff;
  box-shadow: 0 0 16px rgba(0, 212, 255, 0.18);
  text-align: right;
}

.clock-label {
  display: block;
  font-size: 12px;
  color: rgba(255, 255, 255, 0.7);
  margin-bottom: 4px;
}

.clock-value {
  display: block;
  font-size: 14px;
  font-weight: 600;
}
</style>
