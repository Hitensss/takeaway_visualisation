import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import datav from '@kjgl77/datav-vue3'
import './styles/theme.scss'

// 确保 ECharts 全局可用
import * as echarts from 'echarts'
window.echarts = echarts

// 引入DataV
import DataVVue3 from '@kjgl77/datav-vue3'

const app = createApp(App)
app.use(router)
app.use(ElementPlus)
app.use(DataVVue3)
app.mount('#app')
