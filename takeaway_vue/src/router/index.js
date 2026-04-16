import { createRouter, createWebHistory } from 'vue-router'

// 由于我们使用 App.vue 中的 tab 切换组件，路由只需要一个默认页面即可
// 所有大屏都通过组件切换，不走路由
const routes = [
  {
    path: '/',
    redirect: '/dashboard',
  },
  {
    path: '/dashboard',
    name: 'Dashboard',
    component: () => import('@/App.vue'), // 直接使用 App.vue 作为主页面
    meta: { title: '数据大屏' },
  },
  // 如果后续需要单独访问某个大屏，可以保留这些路由（可选）
  {
    path: '/merchant-overview',
    name: 'MerchantOverview',
    component: () => import('@/views/dashboards/MerchantOverview.vue'),
    meta: { title: '外卖商家总览' },
  },

  {
    path: '/delivery-overview',
    name: 'DeliveryOverview',
    component: () => import('@/views/dashboards/DeliveryOverview.vue'),
    meta: { title: '配送总览' },
  },

  {
    path: '/merchant-analysis',
    name: 'MerchantAnalysis',
    component: () => import('@/views/dashboards/MerchantAnalysis.vue'),
    meta: { title: '外卖商家分析' },
  },
  {
    path: '/delivery-analysis',
    name: 'DeliveryAnalysis',
    component: () => import('@/views/dashboards/DeliveryAnalysis.vue'),
    meta: { title: '外卖配送分析' },
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

// 设置页面标题
router.beforeEach((to, from, next) => {
  if (to.meta.title) {
    document.title = `${to.meta.title} - 外卖餐饮数据分析系统`
  }
  next()
})

export default router
