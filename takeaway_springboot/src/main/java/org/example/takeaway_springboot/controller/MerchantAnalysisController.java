package org.example.takeaway_springboot.controller;

import org.example.takeaway_springboot.dto.ApiResponse;
import org.example.takeaway_springboot.entity.*;
import org.example.takeaway_springboot.service.MerchantAnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 商家分析Controller
 * 提供商家品类、评分、销量、价格等分析数据的API接口
 */
@RestController
@RequestMapping("/api/merchant")
@CrossOrigin(origins = "*")
public class MerchantAnalysisController {

    @Autowired
    private MerchantAnalysisService merchantAnalysisService;

    // ==================== 1. 商家品类分析 ====================

    /**
     * 1.1 品类特征统计（横向柱状图）
     * GET /api/merchant/category-stats
     */
    @GetMapping("/category-stats")
    public ApiResponse<List<CategoryStats>> getCategoryStats() {
        try {
            List<CategoryStats> data = merchantAnalysisService.getCategoryStats();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取品类统计失败：" + e.getMessage());
        }
    }

    // ==================== 2. 商家评分分析 ====================

    /**
     * 2.1 评分分布（饼图）
     * GET /api/merchant/rating-distribution
     */
    @GetMapping("/rating-distribution")
    public ApiResponse<List<RatingDistribution>> getRatingDistribution() {
        try {
            List<RatingDistribution> data = merchantAnalysisService.getRatingDistribution();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取评分分布失败：" + e.getMessage());
        }
    }

    /**
     * 2.2 月售-评分散点图 + 趋势线
     * GET /api/merchant/sales-rating-correlation
     */
    @GetMapping("/sales-rating-correlation")
    public ApiResponse<List<SalesRatingCorrelation>> getSalesRatingCorrelation() {
        try {
            List<SalesRatingCorrelation> data = merchantAnalysisService.getSalesRatingCorrelation();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取月售-评分散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 2.3 距离-评分散点图
     * GET /api/merchant/distance-rating-scatter
     */
    @GetMapping("/distance-rating-scatter")
    public ApiResponse<List<DistanceRatingScatter>> getDistanceRatingScatter() {
        try {
            List<DistanceRatingScatter> data = merchantAnalysisService.getDistanceRatingScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取距离-评分散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 2.4 距离-评分箱线图（近/中/远三组对比）
     * GET /api/merchant/distance-rating-boxplot
     */
    @GetMapping("/distance-rating-boxplot")
    public ApiResponse<List<DistanceRatingBoxplot>> getDistanceRatingBoxplot() {
        try {
            List<DistanceRatingBoxplot> data = merchantAnalysisService.getDistanceRatingBoxplot();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取距离-评分箱线图数据失败：" + e.getMessage());
        }
    }

    /**
     * 2.5 人均-评分散点图 + 趋势线
     * GET /api/merchant/price-rating-scatter
     */
    @GetMapping("/price-rating-scatter")
    public ApiResponse<List<PriceRatingScatter>> getPriceRatingScatter() {
        try {
            List<PriceRatingScatter> data = merchantAnalysisService.getPriceRatingScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取人均-评分散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 2.6 人均-评分分组统计（用于分组柱状图）
     * GET /api/merchant/price-rating-group
     */
    @GetMapping("/price-rating-group")
    public ApiResponse<List<PriceRatingGroup>> getPriceRatingGroup() {
        try {
            List<PriceRatingGroup> data = merchantAnalysisService.getPriceRatingGroup();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取人均-评分分组统计失败：" + e.getMessage());
        }
    }

    /**
     * 2.7 品类评分统计（用于词云）
     * GET /api/merchant/category-rating-stats
     */
    @GetMapping("/category-rating-stats")
    public ApiResponse<List<CategoryRatingStats>> getCategoryRatingStats() {
        try {
            List<CategoryRatingStats> data = merchantAnalysisService.getCategoryRatingStats();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取品类评分统计失败：" + e.getMessage());
        }
    }

    /**
     * 2.8 品类词云数据（评分越高字越大）
     * GET /api/merchant/category-wordcloud
     */
    @GetMapping("/category-wordcloud")
    public ApiResponse<List<CategoryWordcloud>> getCategoryWordcloud() {
        try {
            List<CategoryWordcloud> data = merchantAnalysisService.getCategoryWordcloud();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取品类词云数据失败：" + e.getMessage());
        }
    }

    /**
     * 2.9 品类评分分布（堆叠柱状图）
     * GET /api/merchant/category-rating-distribution
     */
    @GetMapping("/category-rating-distribution")
    public ApiResponse<List<CategoryRatingDistribution>> getCategoryRatingDistribution() {
        try {
            List<CategoryRatingDistribution> data = merchantAnalysisService.getCategoryRatingDistribution();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取品类评分分布失败：" + e.getMessage());
        }
    }

    /**
     * 2.10 品类评分箱线图
     * GET /api/merchant/category-rating-boxplot
     */
    @GetMapping("/category-rating-boxplot")
    public ApiResponse<List<CategoryRatingBoxplot>> getCategoryRatingBoxplot() {
        try {
            List<CategoryRatingBoxplot> data = merchantAnalysisService.getCategoryRatingBoxplot();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取品类评分箱线图数据失败：" + e.getMessage());
        }
    }

    /**
     * 2.11 配送费-评分分组柱状图
     * GET /api/merchant/delivery-fee-rating-group
     */
    @GetMapping("/delivery-fee-rating-group")
    public ApiResponse<List<DeliveryFeeRatingGroup>> getDeliveryFeeRatingGroup() {
        try {
            List<DeliveryFeeRatingGroup> data = merchantAnalysisService.getDeliveryFeeRatingGroup();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取配送费-评分分组统计失败：" + e.getMessage());
        }
    }

    /**
     * 2.12 配送费-评分散点图
     * GET /api/merchant/delivery-fee-rating-scatter
     */
    @GetMapping("/delivery-fee-rating-scatter")
    public ApiResponse<List<DeliveryFeeRatingScatter>> getDeliveryFeeRatingScatter() {
        try {
            List<DeliveryFeeRatingScatter> data = merchantAnalysisService.getDeliveryFeeRatingScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取配送费-评分散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 2.13 送达时间-评分散点图
     * GET /api/merchant/delivery-time-rating-scatter
     */
    @GetMapping("/delivery-time-rating-scatter")
    public ApiResponse<List<DeliveryTimeRatingScatter>> getDeliveryTimeRatingScatter() {
        try {
            List<DeliveryTimeRatingScatter> data = merchantAnalysisService.getDeliveryTimeRatingScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取送达时间-评分散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 2.14 送达时间-评分分组统计
     * GET /api/merchant/delivery-time-rating-group
     */
    @GetMapping("/delivery-time-rating-group")
    public ApiResponse<List<DeliveryTimeRatingGroup>> getDeliveryTimeRatingGroup() {
        try {
            List<DeliveryTimeRatingGroup> data = merchantAnalysisService.getDeliveryTimeRatingGroup();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取送达时间-评分分组统计失败：" + e.getMessage());
        }
    }

    /**
     * 2.15 起送价-评分散点图
     * GET /api/merchant/min-price-rating-scatter
     */
    @GetMapping("/min-price-rating-scatter")
    public ApiResponse<List<MinPriceRatingScatter>> getMinPriceRatingScatter() {
        try {
            List<MinPriceRatingScatter> data = merchantAnalysisService.getMinPriceRatingScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取起送价-评分散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 2.16 起送价-评分分组统计
     * GET /api/merchant/min-price-rating-group
     */
    @GetMapping("/min-price-rating-group")
    public ApiResponse<List<MinPriceRatingGroup>> getMinPriceRatingGroup() {
        try {
            List<MinPriceRatingGroup> data = merchantAnalysisService.getMinPriceRatingGroup();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取起送价-评分分组统计失败：" + e.getMessage());
        }
    }

    // ==================== 3. 商家销量分析 ====================

    /**
     * 3.1 月售分布（直方图）
     * GET /api/merchant/sales-distribution
     */
    @GetMapping("/sales-distribution")
    public ApiResponse<List<SalesDistribution>> getSalesDistribution() {
        try {
            List<SalesDistribution> data = merchantAnalysisService.getSalesDistribution();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取月售分布失败：" + e.getMessage());
        }
    }

    /**
     * 3.2 月售统计摘要
     * GET /api/merchant/sales-summary
     */
    @GetMapping("/sales-summary")
    public ApiResponse<List<SalesSummary>> getSalesSummary() {
        try {
            List<SalesSummary> data = merchantAnalysisService.getSalesSummary();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取月售摘要失败：" + e.getMessage());
        }
    }

    /**
     * 3.3 品类月售分布（分组柱状图）
     * GET /api/merchant/category-sales-distribution
     */
    @GetMapping("/category-sales-distribution")
    public ApiResponse<List<CategorySalesDistribution>> getCategorySalesDistribution() {
        try {
            List<CategorySalesDistribution> data = merchantAnalysisService.getCategorySalesDistribution();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取品类月售分布失败：" + e.getMessage());
        }
    }

    /**
     * 3.4 品类月售统计
     * GET /api/merchant/category-sales-stats
     */
    @GetMapping("/category-sales-stats")
    public ApiResponse<List<CategorySalesStats>> getCategorySalesStats() {
        try {
            List<CategorySalesStats> data = merchantAnalysisService.getCategorySalesStats();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取品类月售统计失败：" + e.getMessage());
        }
    }

    /**
     * 3.5 月售-配送费散点图
     * GET /api/merchant/sales-delivery-fee-scatter
     */
    @GetMapping("/sales-delivery-fee-scatter")
    public ApiResponse<List<SalesDeliveryFeeScatter>> getSalesDeliveryFeeScatter() {
        try {
            List<SalesDeliveryFeeScatter> data = merchantAnalysisService.getSalesDeliveryFeeScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取月售-配送费散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 3.6 配送费分组月售统计
     * GET /api/merchant/delivery-fee-sales-group
     */
    @GetMapping("/delivery-fee-sales-group")
    public ApiResponse<List<DeliveryFeeSalesGroup>> getDeliveryFeeSalesGroup() {
        try {
            List<DeliveryFeeSalesGroup> data = merchantAnalysisService.getDeliveryFeeSalesGroup();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取配送费分组月售统计失败：" + e.getMessage());
        }
    }

    /**
     * 3.7 月售-送达时间散点图
     * GET /api/merchant/sales-delivery-time-scatter
     */
    @GetMapping("/sales-delivery-time-scatter")
    public ApiResponse<List<SalesDeliveryTimeScatter>> getSalesDeliveryTimeScatter() {
        try {
            List<SalesDeliveryTimeScatter> data = merchantAnalysisService.getSalesDeliveryTimeScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取月售-送达时间散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 3.8 送达时间分组月售统计
     * GET /api/merchant/delivery-time-sales-group
     */
    @GetMapping("/delivery-time-sales-group")
    public ApiResponse<List<DeliveryTimeSalesGroup>> getDeliveryTimeSalesGroup() {
        try {
            List<DeliveryTimeSalesGroup> data = merchantAnalysisService.getDeliveryTimeSalesGroup();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取送达时间分组月售统计失败：" + e.getMessage());
        }
    }

    /**
     * 3.9 月售-距离散点图
     * GET /api/merchant/sales-distance-scatter
     */
    @GetMapping("/sales-distance-scatter")
    public ApiResponse<List<SalesDistanceScatter>> getSalesDistanceScatter() {
        try {
            List<SalesDistanceScatter> data = merchantAnalysisService.getSalesDistanceScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取月售-距离散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 3.10 距离分组月售统计
     * GET /api/merchant/distance-sales-group
     */
    @GetMapping("/distance-sales-group")
    public ApiResponse<List<DistanceSalesGroup>> getDistanceSalesGroup() {
        try {
            List<DistanceSalesGroup> data = merchantAnalysisService.getDistanceSalesGroup();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取距离分组月售统计失败：" + e.getMessage());
        }
    }

    /**
     * 3.11 Top10店铺（水平条形图）
     * GET /api/merchant/top-shops
     */
    @GetMapping("/top-shops")
    public ApiResponse<List<TopShops>> getTopShops() {
        try {
            List<TopShops> data = merchantAnalysisService.getTopShops();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取Top10店铺失败：" + e.getMessage());
        }
    }

    // ==================== 4. 商家价格分析 ====================

    /**
     * 4.1 人均消费分布（直方图）
     * GET /api/merchant/price-distribution
     */
    @GetMapping("/price-distribution")
    public ApiResponse<List<PriceDistribution>> getPriceDistribution() {
        try {
            List<PriceDistribution> data = merchantAnalysisService.getPriceDistribution();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取人均消费分布失败：" + e.getMessage());
        }
    }

    /**
     * 4.2 人均消费统计摘要
     * GET /api/merchant/price-summary
     */
    @GetMapping("/price-summary")
    public ApiResponse<List<PriceSummary>> getPriceSummary() {
        try {
            List<PriceSummary> data = merchantAnalysisService.getPriceSummary();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取人均消费摘要失败：" + e.getMessage());
        }
    }

    /**
     * 4.3 起送价分布（直方图）
     * GET /api/merchant/min-price-distribution
     */
    @GetMapping("/min-price-distribution")
    public ApiResponse<List<MinPriceDistribution>> getMinPriceDistribution() {
        try {
            List<MinPriceDistribution> data = merchantAnalysisService.getMinPriceDistribution();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取起送价分布失败：" + e.getMessage());
        }
    }

    /**
     * 4.4 起送价统计摘要
     * GET /api/merchant/min-price-summary
     */
    @GetMapping("/min-price-summary")
    public ApiResponse<List<MinPriceSummary>> getMinPriceSummary() {
        try {
            List<MinPriceSummary> data = merchantAnalysisService.getMinPriceSummary();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取起送价摘要失败：" + e.getMessage());
        }
    }
}