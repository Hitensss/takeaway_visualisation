package org.example.takeaway_springboot.service.impl;

import org.example.takeaway_springboot.entity.*;
import org.example.takeaway_springboot.mapper.*;
import org.example.takeaway_springboot.service.MerchantAnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MerchantAnalysisServiceImpl implements MerchantAnalysisService {

    // ==================== 1. 商家品类分析 Mapper ====================
    @Autowired
    private CategoryStatsMapper categoryStatsMapper;

    // ==================== 2. 商家评分分析 Mapper ====================
    @Autowired
    private RatingDistributionMapper ratingDistributionMapper;
    @Autowired
    private SalesRatingCorrelationMapper salesRatingCorrelationMapper;
    @Autowired
    private DistanceRatingScatterMapper distanceRatingScatterMapper;
    @Autowired
    private DistanceRatingBoxplotMapper distanceRatingBoxplotMapper;
    @Autowired
    private PriceRatingScatterMapper priceRatingScatterMapper;
    @Autowired
    private PriceRatingGroupMapper priceRatingGroupMapper;
    @Autowired
    private CategoryRatingStatsMapper categoryRatingStatsMapper;
    @Autowired
    private CategoryWordcloudMapper categoryWordcloudMapper;
    @Autowired
    private CategoryRatingDistributionMapper categoryRatingDistributionMapper;
    @Autowired
    private CategoryRatingBoxplotMapper categoryRatingBoxplotMapper;
    @Autowired
    private DeliveryFeeRatingGroupMapper deliveryFeeRatingGroupMapper;
    @Autowired
    private DeliveryFeeRatingScatterMapper deliveryFeeRatingScatterMapper;
    @Autowired
    private DeliveryTimeRatingScatterMapper deliveryTimeRatingScatterMapper;
    @Autowired
    private DeliveryTimeRatingGroupMapper deliveryTimeRatingGroupMapper;
    @Autowired
    private MinPriceRatingScatterMapper minPriceRatingScatterMapper;
    @Autowired
    private MinPriceRatingGroupMapper minPriceRatingGroupMapper;

    // ==================== 3. 商家销量分析 Mapper ====================
    @Autowired
    private SalesDistributionMapper salesDistributionMapper;
    @Autowired
    private SalesSummaryMapper salesSummaryMapper;
    @Autowired
    private CategorySalesDistributionMapper categorySalesDistributionMapper;
    @Autowired
    private CategorySalesStatsMapper categorySalesStatsMapper;
    @Autowired
    private SalesDeliveryFeeScatterMapper salesDeliveryFeeScatterMapper;
    @Autowired
    private DeliveryFeeSalesGroupMapper deliveryFeeSalesGroupMapper;
    @Autowired
    private SalesDeliveryTimeScatterMapper salesDeliveryTimeScatterMapper;
    @Autowired
    private DeliveryTimeSalesGroupMapper deliveryTimeSalesGroupMapper;
    @Autowired
    private SalesDistanceScatterMapper salesDistanceScatterMapper;
    @Autowired
    private DistanceSalesGroupMapper distanceSalesGroupMapper;
    @Autowired
    private TopShopsMapper topShopsMapper;

    // ==================== 4. 商家价格分析 Mapper ====================
    @Autowired
    private PriceDistributionMapper priceDistributionMapper;
    @Autowired
    private PriceSummaryMapper priceSummaryMapper;
    @Autowired
    private MinPriceDistributionMapper minPriceDistributionMapper;
    @Autowired
    private MinPriceSummaryMapper minPriceSummaryMapper;

    // ==================== 1. 商家品类分析 ====================
    @Override
    public List<CategoryStats> getCategoryStats() {
        return categoryStatsMapper.findAll();
    }

    // ==================== 2. 商家评分分析 ====================
    @Override
    public List<RatingDistribution> getRatingDistribution() {
        return ratingDistributionMapper.findAll();
    }

    @Override
    public List<SalesRatingCorrelation> getSalesRatingCorrelation() {
        return salesRatingCorrelationMapper.findAll();
    }

    @Override
    public List<DistanceRatingScatter> getDistanceRatingScatter() {
        return distanceRatingScatterMapper.findAll();
    }

    @Override
    public List<DistanceRatingBoxplot> getDistanceRatingBoxplot() {
        return distanceRatingBoxplotMapper.findAll();
    }

    @Override
    public List<PriceRatingScatter> getPriceRatingScatter() {
        return priceRatingScatterMapper.findAll();
    }

    @Override
    public List<PriceRatingGroup> getPriceRatingGroup() {
        return priceRatingGroupMapper.findAll();
    }

    @Override
    public List<CategoryRatingStats> getCategoryRatingStats() {
        return categoryRatingStatsMapper.findAll();
    }

    @Override
    public List<CategoryWordcloud> getCategoryWordcloud() {
        return categoryWordcloudMapper.findAll();
    }

    @Override
    public List<CategoryRatingDistribution> getCategoryRatingDistribution() {
        return categoryRatingDistributionMapper.findAll();
    }

    @Override
    public List<CategoryRatingBoxplot> getCategoryRatingBoxplot() {
        return categoryRatingBoxplotMapper.findAll();
    }

    @Override
    public List<DeliveryFeeRatingGroup> getDeliveryFeeRatingGroup() {
        return deliveryFeeRatingGroupMapper.findAll();
    }

    @Override
    public List<DeliveryFeeRatingScatter> getDeliveryFeeRatingScatter() {
        return deliveryFeeRatingScatterMapper.findAll();
    }

    @Override
    public List<DeliveryTimeRatingScatter> getDeliveryTimeRatingScatter() {
        return deliveryTimeRatingScatterMapper.findAll();
    }

    @Override
    public List<DeliveryTimeRatingGroup> getDeliveryTimeRatingGroup() {
        return deliveryTimeRatingGroupMapper.findAll();
    }

    @Override
    public List<MinPriceRatingScatter> getMinPriceRatingScatter() {
        return minPriceRatingScatterMapper.findAll();
    }

    @Override
    public List<MinPriceRatingGroup> getMinPriceRatingGroup() {
        return minPriceRatingGroupMapper.findAll();
    }

    // ==================== 3. 商家销量分析 ====================
    @Override
    public List<SalesDistribution> getSalesDistribution() {
        return salesDistributionMapper.findAll();
    }

    @Override
    public List<SalesSummary> getSalesSummary() {
        return salesSummaryMapper.findAll();
    }

    @Override
    public List<CategorySalesDistribution> getCategorySalesDistribution() {
        return categorySalesDistributionMapper.findAll();
    }

    @Override
    public List<CategorySalesStats> getCategorySalesStats() {
        return categorySalesStatsMapper.findAll();
    }

    @Override
    public List<SalesDeliveryFeeScatter> getSalesDeliveryFeeScatter() {
        return salesDeliveryFeeScatterMapper.findAll();
    }

    @Override
    public List<DeliveryFeeSalesGroup> getDeliveryFeeSalesGroup() {
        return deliveryFeeSalesGroupMapper.findAll();
    }

    @Override
    public List<SalesDeliveryTimeScatter> getSalesDeliveryTimeScatter() {
        return salesDeliveryTimeScatterMapper.findAll();
    }

    @Override
    public List<DeliveryTimeSalesGroup> getDeliveryTimeSalesGroup() {
        return deliveryTimeSalesGroupMapper.findAll();
    }

    @Override
    public List<SalesDistanceScatter> getSalesDistanceScatter() {
        return salesDistanceScatterMapper.findAll();
    }

    @Override
    public List<DistanceSalesGroup> getDistanceSalesGroup() {
        return distanceSalesGroupMapper.findAll();
    }

    @Override
    public List<TopShops> getTopShops() {
        return topShopsMapper.findAll();
    }

    // ==================== 4. 商家价格分析 ====================
    @Override
    public List<PriceDistribution> getPriceDistribution() {
        return priceDistributionMapper.findAll();
    }

    @Override
    public List<PriceSummary> getPriceSummary() {
        return priceSummaryMapper.findAll();
    }

    @Override
    public List<MinPriceDistribution> getMinPriceDistribution() {
        return minPriceDistributionMapper.findAll();
    }

    @Override
    public List<MinPriceSummary> getMinPriceSummary() {
        return minPriceSummaryMapper.findAll();
    }
}