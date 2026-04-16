package org.example.takeaway_springboot.service;

import org.example.takeaway_springboot.entity.*;

import java.util.List;

public interface MerchantAnalysisService {

    // ==================== 1. 商家品类分析 ====================
    List<CategoryStats> getCategoryStats();

    // ==================== 2. 商家评分分析 ====================
    List<RatingDistribution> getRatingDistribution();
    List<SalesRatingCorrelation> getSalesRatingCorrelation();
    List<DistanceRatingScatter> getDistanceRatingScatter();
    List<DistanceRatingBoxplot> getDistanceRatingBoxplot();
    List<PriceRatingScatter> getPriceRatingScatter();
    List<PriceRatingGroup> getPriceRatingGroup();
    List<CategoryRatingStats> getCategoryRatingStats();
    List<CategoryWordcloud> getCategoryWordcloud();
    List<CategoryRatingDistribution> getCategoryRatingDistribution();
    List<CategoryRatingBoxplot> getCategoryRatingBoxplot();
    List<DeliveryFeeRatingGroup> getDeliveryFeeRatingGroup();
    List<DeliveryFeeRatingScatter> getDeliveryFeeRatingScatter();
    List<DeliveryTimeRatingScatter> getDeliveryTimeRatingScatter();
    List<DeliveryTimeRatingGroup> getDeliveryTimeRatingGroup();
    List<MinPriceRatingScatter> getMinPriceRatingScatter();
    List<MinPriceRatingGroup> getMinPriceRatingGroup();

    // ==================== 3. 商家销量分析 ====================
    List<SalesDistribution> getSalesDistribution();
    List<SalesSummary> getSalesSummary();
    List<CategorySalesDistribution> getCategorySalesDistribution();
    List<CategorySalesStats> getCategorySalesStats();
    List<SalesDeliveryFeeScatter> getSalesDeliveryFeeScatter();
    List<DeliveryFeeSalesGroup> getDeliveryFeeSalesGroup();
    List<SalesDeliveryTimeScatter> getSalesDeliveryTimeScatter();
    List<DeliveryTimeSalesGroup> getDeliveryTimeSalesGroup();
    List<SalesDistanceScatter> getSalesDistanceScatter();
    List<DistanceSalesGroup> getDistanceSalesGroup();
    List<TopShops> getTopShops();

    // ==================== 4. 商家价格分析 ====================
    List<PriceDistribution> getPriceDistribution();
    List<PriceSummary> getPriceSummary();
    List<MinPriceDistribution> getMinPriceDistribution();
    List<MinPriceSummary> getMinPriceSummary();
}
