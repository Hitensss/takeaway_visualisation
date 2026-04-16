import axios from 'axios'

const request = axios.create({
  baseURL: '/api',
  timeout: 30000,
})

// ==================== 商家分析模块 ====================
export const merchantAPI = {
  // 品类特征统计
  getCategoryStats: () => request.get('/merchant/category-stats'),
  // 评分分布
  getRatingDistribution: () => request.get('/merchant/rating-distribution'),
  // 月售-评分散点图
  getSalesRatingCorrelation: () => request.get('/merchant/sales-rating-correlation'),
  // 距离-评分散点图
  getDistanceRatingScatter: () => request.get('/merchant/distance-rating-scatter'),
  // 距离-评分箱线图
  getDistanceRatingBoxplot: () => request.get('/merchant/distance-rating-boxplot'),
  // 人均-评分散点图
  getPriceRatingScatter: () => request.get('/merchant/price-rating-scatter'),
  // 人均-评分分组统计
  getPriceRatingGroup: () => request.get('/merchant/price-rating-group'),
  // 品类评分统计
  getCategoryRatingStats: () => request.get('/merchant/category-rating-stats'),
  // 品类词云
  getCategoryWordcloud: () => request.get('/merchant/category-wordcloud'),
  // 品类评分分布
  getCategoryRatingDistribution: () => request.get('/merchant/category-rating-distribution'),
  // 品类评分箱线图
  getCategoryRatingBoxplot: () => request.get('/merchant/category-rating-boxplot'),
  // 配送费-评分分组
  getDeliveryFeeRatingGroup: () => request.get('/merchant/delivery-fee-rating-group'),
  // 配送费-评分散点图
  getDeliveryFeeRatingScatter: () => request.get('/merchant/delivery-fee-rating-scatter'),
  // 送达时间-评分散点图
  getDeliveryTimeRatingScatter: () => request.get('/merchant/delivery-time-rating-scatter'),
  // 送达时间-评分分组
  getDeliveryTimeRatingGroup: () => request.get('/merchant/delivery-time-rating-group'),
  // 起送价-评分散点图
  getMinPriceRatingScatter: () => request.get('/merchant/min-price-rating-scatter'),
  // 起送价-评分分组
  getMinPriceRatingGroup: () => request.get('/merchant/min-price-rating-group'),
  // 月售分布
  getSalesDistribution: () => request.get('/merchant/sales-distribution'),
  // 月售摘要
  getSalesSummary: () => request.get('/merchant/sales-summary'),
  // 品类月售分布
  getCategorySalesDistribution: () => request.get('/merchant/category-sales-distribution'),
  // 品类月售统计
  getCategorySalesStats: () => request.get('/merchant/category-sales-stats'),
  // 月售-配送费散点图
  getSalesDeliveryFeeScatter: () => request.get('/merchant/sales-delivery-fee-scatter'),
  // 配送费分组月售
  getDeliveryFeeSalesGroup: () => request.get('/merchant/delivery-fee-sales-group'),
  // 月售-送达时间散点图
  getSalesDeliveryTimeScatter: () => request.get('/merchant/sales-delivery-time-scatter'),
  // 送达时间分组月售
  getDeliveryTimeSalesGroup: () => request.get('/merchant/delivery-time-sales-group'),
  // 月售-距离散点图
  getSalesDistanceScatter: () => request.get('/merchant/sales-distance-scatter'),
  // 距离分组月售
  getDistanceSalesGroup: () => request.get('/merchant/distance-sales-group'),
  // Top10店铺
  getTopShops: () => request.get('/merchant/top-shops'),
  // 人均分布
  getPriceDistribution: () => request.get('/merchant/price-distribution'),
  // 人均摘要
  getPriceSummary: () => request.get('/merchant/price-summary'),
  // 起送价分布
  getMinPriceDistribution: () => request.get('/merchant/min-price-distribution'),
  // 起送价摘要
  getMinPriceSummary: () => request.get('/merchant/min-price-summary'),
}

// ==================== 外卖配送分析模块 ====================
export const deliveryAPI = {
  getDistanceStats: () => request.get('/delivery/distance-stats'),
  getCategoryDistanceBoxplot: () => request.get('/delivery/category-distance-boxplot'),
  getDeliveryTimeDistribution: () => request.get('/delivery/delivery-time-distribution'),
  getDeliveryTimeSummary: () => request.get('/delivery/delivery-time-summary'),
  getDeliveryFeeDistribution: () => request.get('/delivery/delivery-fee-distribution'),
  getDeliveryFeeSummary: () => request.get('/delivery/delivery-fee-summary'),
  getFeeDistanceScatter: () => request.get('/delivery/fee-distance-scatter'),
  getDistanceFeeGroup: () => request.get('/delivery/distance-fee-group'),
  getFeeTimeScatter: () => request.get('/delivery/fee-time-scatter'),
  getFeeTimeGroup: () => request.get('/delivery/fee-time-group'),
  getCorrelationMatrix: () => request.get('/delivery/correlation-matrix'),
  getCorrelationSummary: () => request.get('/delivery/correlation-summary'),
}
