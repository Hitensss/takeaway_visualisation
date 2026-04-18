<template>
  <div class="admin-panel">
    <div class="header">
      <h2>⚙️ 后台管理</h2>
      <button class="back-btn" @click="$router.push('/')">← 返回大屏</button>
    </div>

    <div class="two-columns">
      <!-- 文件上传区域 -->
      <div class="card">
        <h3>📤 上传数据文件</h3>
        <div class="upload-area" @dragover.prevent @drop.prevent="handleDrop">
          <input
            type="file"
            ref="fileInput"
            @change="handleFileSelect"
            accept=".csv"
            style="display: none"
          />
          <button class="upload-btn" @click="$refs.fileInput.click()">📁 选择CSV文件</button>
          <span class="upload-tip">或将文件拖拽到此区域</span>
        </div>

        <div v-if="uploading" class="upload-progress">
          <div class="spinner"></div>
          <span>上传中，请稍候...</span>
        </div>

        <div v-if="uploadResult" class="upload-result" :class="uploadResult.type">
          {{ uploadResult.message }}
        </div>
      </div>

      <!-- HDFS文件列表 -->
      <div class="card">
        <div class="section-header">
          <h3>📁 HDFS文件列表</h3>
          <button class="refresh-btn" @click="loadFiles">🔄 刷新</button>
        </div>

        <div class="file-list">
          <div v-for="file in fileList" :key="file.name" class="file-item">
            <div class="file-info">
              <span class="file-name">📄 {{ file.name }}</span>
              <span class="file-size">{{ formatFileSize(file.size) }}</span>
              <span class="file-time">{{ formatTime(file.modificationTime) }}</span>
            </div>
            <div class="file-actions">
              <button class="delete-btn" @click="deleteFile(file.name)">🗑️ 删除</button>
            </div>
          </div>
          <div v-if="fileList.length === 0" class="empty-state">暂无文件，请上传CSV文件</div>
        </div>
      </div>
    </div>

    <!-- 两列布局：Spark任务状态 + 使用说明 -->
    <div class="two-columns">
      <!-- 左侧：Spark任务状态 -->
      <div class="card left-card">
        <div class="section-header">
          <h3>📊 Spark任务状态</h3>
          <button class="refresh-btn" @click="loadTasks">🔄 刷新</button>
        </div>

        <div class="task-list">
          <div
            v-for="(task, id) in tasks"
            :key="id"
            class="task-item"
            :class="task.status.toLowerCase()"
          >
            <div class="task-header">
              <div class="task-info">
                <span class="task-name">📄 {{ truncateId(id) }}</span>
                <span class="task-status-badge" :class="task.status.toLowerCase()">
                  {{ task.status }}
                </span>
              </div>
              <div class="task-time">
                ⏱️ 开始: {{ formatTime(task.startTime) }}
                <span v-if="task.endTime"> | 结束: {{ formatTime(task.endTime) }}</span>
                <span v-if="task.endTime && task.startTime">
                  | 耗时: {{ formatDuration(task.endTime - task.startTime) }}
                </span>
              </div>
            </div>
            <div class="task-log">
              <details>
                <summary>📋 查看运行日志</summary>
                <pre class="log-content">{{ task.log || task.error || '无日志输出' }}</pre>
              </details>
            </div>
          </div>
          <div v-if="Object.keys(tasks).length === 0" class="empty-state">
            暂无Spark任务记录，上传文件后会自动触发
          </div>
        </div>
      </div>

      <!-- 右侧：使用说明 -->
      <div class="card right-card">
        <h3>📖 使用说明</h3>
        <div class="tips">
          <p>1️⃣ 上传CSV文件后，系统会自动将文件存入HDFS</p>
          <p>2️⃣ 上传完成后，系统会自动触发Spark分析任务</p>
          <p>3️⃣ 可以在左侧「Spark任务状态」区域查看运行进度和日志</p>
          <p>4️⃣ 分析完成后，返回大屏页面即可看到更新后的数据</p>
          <p>
            5️⃣
            CSV文件格式要求：包含商家名称、品类、评分、月售、距离、人均、起送价、配送费、送达时间字段
          </p>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { adminAPI } from '@/api'

const fileInput = ref(null)
const fileList = ref([])
const tasks = ref({})
const uploading = ref(false)
const uploadResult = ref(null)

let refreshTimer = null

// 加载HDFS文件列表
const loadFiles = async () => {
  try {
    const res = await adminAPI.getFileList()
    if (res.data.code === 200) {
      fileList.value = res.data.data || []
    }
  } catch (error) {
    console.error('加载文件列表失败:', error)
  }
}

// 加载Spark任务状态
const loadTasks = async () => {
  try {
    const res = await adminAPI.getTaskStatus()
    if (res.data.code === 200) {
      tasks.value = res.data.data || {}
    }
  } catch (error) {
    console.error('加载任务状态失败:', error)
  }
}

// 上传文件
const uploadFile = async (file) => {
  uploading.value = true
  uploadResult.value = null

  const formData = new FormData()
  formData.append('file', file)

  try {
    const res = await adminAPI.uploadFile(formData)
    if (res.data.code === 200) {
      // 上传成功，立即显示成功信息
      uploadResult.value = {
        type: 'success',
        message: `✅ ${file.name} 已上传，Spark分析正在后台执行，请查看任务状态`,
      }
      await loadFiles()
      await loadTasks()

      // 开始轮询任务状态
      startPollingTasks()
    } else {
      uploadResult.value = { type: 'error', message: `❌ 上传失败: ${res.data.message}` }
    }
  } catch (error) {
    uploadResult.value = { type: 'error', message: `❌ 上传失败: ${error.message}` }
  } finally {
    uploading.value = false
  }
}

// 轮询任务状态（每5秒）
let pollingTimer = null

const startPollingTasks = () => {
  if (pollingTimer) clearInterval(pollingTimer)
  pollingTimer = setInterval(() => {
    loadTasks()
  }, 5000)
}

// 页面销毁时清除定时器
onUnmounted(() => {
  if (pollingTimer) {
    clearInterval(pollingTimer)
  }
})

// 删除文件
const deleteFile = async (fileName) => {
  if (confirm(`确定要删除 ${fileName} 吗？`)) {
    try {
      const res = await adminAPI.deleteFile(fileName)
      if (res.data.code === 200) {
        await loadFiles()
        alert('删除成功')
      } else {
        alert('删除失败: ' + res.data.message)
      }
    } catch (error) {
      alert('删除失败: ' + error.message)
    }
  }
}

// 文件选择处理
const handleFileSelect = (e) => {
  const file = e.target.files[0]
  if (file && file.name.endsWith('.csv')) {
    uploadFile(file)
  } else {
    alert('请选择CSV文件')
  }
  fileInput.value.value = ''
}

// 拖拽上传
const handleDrop = (e) => {
  const file = e.dataTransfer.files[0]
  if (file && file.name.endsWith('.csv')) {
    uploadFile(file)
  } else {
    alert('请拖拽CSV文件')
  }
}

// 格式化文件大小
const formatFileSize = (bytes) => {
  if (!bytes) return '0 B'
  if (bytes < 1024) return bytes + ' B'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB'
  return (bytes / (1024 * 1024)).toFixed(2) + ' MB'
}

// 格式化时间戳
const formatTime = (timestamp) => {
  if (!timestamp) return '--'
  return new Date(timestamp).toLocaleString()
}

// 格式化耗时
const formatDuration = (ms) => {
  if (!ms) return '--'
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return seconds + ' 秒'
  const minutes = Math.floor(seconds / 60)
  const remainSeconds = seconds % 60
  return minutes + ' 分 ' + remainSeconds + ' 秒'
}

// 截断过长的ID
const truncateId = (id) => {
  if (id.length > 50) {
    return id.substring(0, 40) + '...'
  }
  return id
}

onMounted(() => {
  loadFiles()
  loadTasks()
  refreshTimer = setInterval(loadTasks, 5000)
})

onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
  }
})
</script>

<style scoped>
.admin-panel {
  padding: 24px;
  min-height: 100vh;
  background: linear-gradient(135deg, #0a1628 0%, #07111f 100%);
  color: #fff;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
}

.header h2 {
  font-size: 24px;
  color: #00d4ff;
  margin: 0;
}

.back-btn {
  padding: 8px 20px;
  background: rgba(0, 212, 255, 0.2);
  border: 1px solid #00d4ff;
  border-radius: 8px;
  color: #00d4ff;
  cursor: pointer;
  transition: all 0.3s;
}

.back-btn:hover {
  background: rgba(0, 212, 255, 0.4);
}

.card {
  background: rgba(6, 30, 55, 0.6);
  border-radius: 16px;
  padding: 24px;
  margin-bottom: 24px;
  backdrop-filter: blur(5px);
}

.card h3 {
  margin: 0 0 16px 0;
  color: #00d4ff;
  font-size: 18px;
}

/* 两列布局 */
.two-columns {
  display: flex;
  gap: 24px;
  margin-bottom: 0;
}

.left-card {
  flex: 2;
  margin-bottom: 0;
}

.right-card {
  flex: 1;
  margin-bottom: 0;
}

/* 上传区域 */
.upload-area {
  display: flex;
  gap: 20px;
  align-items: center;
  padding: 30px;
  border: 2px dashed #00d4ff;
  border-radius: 12px;
  margin-top: 16px;
}

.upload-btn {
  padding: 12px 24px;
  background: #00d4ff;
  border: none;
  border-radius: 8px;
  color: #0a1628;
  font-weight: bold;
  cursor: pointer;
  transition: all 0.3s;
}

.upload-btn:hover {
  background: #00b8e6;
  transform: scale(1.02);
}

.upload-tip {
  color: #a0c0e0;
}

.upload-progress {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-top: 16px;
  padding: 12px;
  background: rgba(0, 212, 255, 0.1);
  border-radius: 8px;
}

.upload-result {
  margin-top: 16px;
  padding: 12px;
  border-radius: 8px;
}

.upload-result.success {
  background: rgba(46, 204, 113, 0.2);
  border: 1px solid #2ecc71;
  color: #2ecc71;
}

.upload-result.error {
  background: rgba(231, 76, 60, 0.2);
  border: 1px solid #e74c3c;
  color: #e74c3c;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.refresh-btn {
  padding: 6px 16px;
  background: rgba(0, 212, 255, 0.2);
  border: 1px solid #00d4ff;
  border-radius: 6px;
  color: #00d4ff;
  cursor: pointer;
  transition: all 0.3s;
}

.refresh-btn:hover {
  background: rgba(0, 212, 255, 0.4);
}

/* 文件列表 */
.file-list {
  height: 250px;
  max-height: 300px;
  overflow-y: auto;
}

.file-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px;
  border-bottom: 1px solid rgba(0, 212, 255, 0.2);
}

.file-item:hover {
  background: rgba(0, 212, 255, 0.05);
}

.file-info {
  display: flex;
  gap: 20px;
  align-items: center;
  flex-wrap: wrap;
}

.file-name {
  font-weight: bold;
  color: #00d4ff;
}

.file-size,
.file-time {
  font-size: 12px;
  color: #a0c0e0;
}

.delete-btn {
  padding: 6px 16px;
  background: rgba(231, 76, 60, 0.2);
  border: 1px solid #e74c3c;
  border-radius: 6px;
  color: #e74c3c;
  cursor: pointer;
  transition: all 0.3s;
}

.delete-btn:hover {
  background: rgba(231, 76, 60, 0.4);
}

.empty-state {
  text-align: center;
  padding: 40px;
  color: #a0c0e0;
}

/* 任务状态样式 */
.task-list {
  max-height: 500px;
  overflow-y: auto;
}

.task-item {
  background: rgba(6, 30, 55, 0.4);
  border-radius: 12px;
  padding: 16px;
  margin-bottom: 12px;
  border-left: 4px solid #a0c0e0;
}

.task-item.success {
  border-left-color: #2ecc71;
}

.task-item.failed {
  border-left-color: #e74c3c;
}

.task-item.running {
  border-left-color: #f39c12;
}

.task-header {
  margin-bottom: 12px;
}

.task-info {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
  flex-wrap: wrap;
  gap: 8px;
}

.task-name {
  font-weight: bold;
  color: #00d4ff;
  word-break: break-all;
  font-size: 13px;
}

.task-status-badge {
  display: inline-block;
  padding: 4px 12px;
  border-radius: 20px;
  font-size: 12px;
  font-weight: bold;
}

.task-status-badge.success {
  background: rgba(46, 204, 113, 0.2);
  color: #2ecc71;
}

.task-status-badge.failed {
  background: rgba(231, 76, 60, 0.2);
  color: #e74c3c;
}

.task-status-badge.running {
  background: rgba(243, 156, 18, 0.2);
  color: #f39c12;
}

.task-time {
  font-size: 11px;
  color: #a0c0e0;
}

.task-log details {
  margin-top: 8px;
  cursor: pointer;
}

.task-log summary {
  color: #a0c0e0;
  font-size: 12px;
  padding: 4px 0;
}

.task-log summary:hover {
  color: #00d4ff;
}

.log-content {
  background: rgba(0, 0, 0, 0.4);
  padding: 12px;
  border-radius: 8px;
  font-size: 11px;
  font-family: 'Consolas', 'Monaco', monospace;
  color: #a0c0e0;
  overflow-x: auto;
  max-height: 200px;
  margin-top: 8px;
  white-space: pre-wrap;
  word-break: break-all;
}

/* 使用说明区域 */
.tips {
  color: #a0c0e0;
  line-height: 1.8;
}

.tips p {
  margin: 12px 0;
  padding: 8px 12px;
  background: rgba(0, 212, 255, 0.05);
  border-radius: 8px;
}

/* 滚动条样式 */
.file-list::-webkit-scrollbar,
.task-list::-webkit-scrollbar {
  width: 6px;
}

.file-list::-webkit-scrollbar-track,
.task-list::-webkit-scrollbar-track {
  background: rgba(0, 212, 255, 0.1);
  border-radius: 3px;
}

.file-list::-webkit-scrollbar-thumb,
.task-list::-webkit-scrollbar-thumb {
  background: #00d4ff;
  border-radius: 3px;
}

.spinner {
  width: 20px;
  height: 20px;
  border: 2px solid rgba(0, 212, 255, 0.3);
  border-top-color: #00d4ff;
  border-radius: 50%;
  animation: spin 1s linear infinite;
  display: inline-block;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

/* 响应式：小屏幕时两列变一列 */
@media (max-width: 1000px) {
  .two-columns {
    flex-direction: column;
  }
  .left-card,
  .right-card {
    margin-bottom: 24px;
  }
}
</style>
