package org.example.takeaway_springboot.service.impl;

import org.example.takeaway_springboot.entity.*;
import org.example.takeaway_springboot.mapper.*;
import org.example.takeaway_springboot.service.DeliveryAnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DeliveryAnalysisServiceImpl implements DeliveryAnalysisService {

    // ==================== 1. 配送距离分析 Mapper ====================
    @Autowired
    private DistanceStatsMapper distanceStatsMapper;
    @Autowired
    private CategoryDistanceBoxplotMapper categoryDistanceBoxplotMapper;

    // ==================== 2. 送达时间分析 Mapper ====================
    @Autowired
    private DeliveryTimeDistributionMapper deliveryTimeDistributionMapper;
    @Autowired
    private DeliveryTimeSummaryMapper deliveryTimeSummaryMapper;

    // ==================== 3. 配送费分析 Mapper ====================
    @Autowired
    private DeliveryFeeDistributionMapper deliveryFeeDistributionMapper;
    @Autowired
    private DeliveryFeeSummaryMapper deliveryFeeSummaryMapper;
    @Autowired
    private FeeDistanceScatterMapper feeDistanceScatterMapper;
    @Autowired
    private DistanceFeeGroupMapper distanceFeeGroupMapper;
    @Autowired
    private FeeTimeScatterMapper feeTimeScatterMapper;
    @Autowired
    private FeeTimeGroupMapper feeTimeGroupMapper;

    // ==================== 4. 相关性分析 Mapper ====================
    @Autowired
    private CorrelationMatrixMapper correlationMatrixMapper;
    @Autowired
    private CorrelationSummaryMapper correlationSummaryMapper;

    // ==================== 1. 配送距离分析 ====================
    @Override
    public List<DistanceStats> getDistanceStats() {
        return distanceStatsMapper.findAll();
    }

    @Override
    public List<CategoryDistanceBoxplot> getCategoryDistanceBoxplot() {
        return categoryDistanceBoxplotMapper.findAll();
    }

    // ==================== 2. 送达时间分析 ====================
    @Override
    public List<DeliveryTimeDistribution> getDeliveryTimeDistribution() {
        return deliveryTimeDistributionMapper.findAll();
    }

    @Override
    public List<DeliveryTimeSummary> getDeliveryTimeSummary() {
        return deliveryTimeSummaryMapper.findAll();
    }

    // ==================== 3. 配送费分析 ====================
    @Override
    public List<DeliveryFeeDistribution> getDeliveryFeeDistribution() {
        return deliveryFeeDistributionMapper.findAll();
    }

    @Override
    public List<DeliveryFeeSummary> getDeliveryFeeSummary() {
        return deliveryFeeSummaryMapper.findAll();
    }

    @Override
    public List<FeeDistanceScatter> getFeeDistanceScatter() {
        return feeDistanceScatterMapper.findAll();
    }

    @Override
    public List<DistanceFeeGroup> getDistanceFeeGroup() {
        return distanceFeeGroupMapper.findAll();
    }

    @Override
    public List<FeeTimeScatter> getFeeTimeScatter() {
        return feeTimeScatterMapper.findAll();
    }

    @Override
    public List<FeeTimeGroup> getFeeTimeGroup() {
        return feeTimeGroupMapper.findAll();
    }

    // ==================== 4. 相关性分析 ====================
    @Override
    public List<CorrelationMatrix> getCorrelationMatrix() {
        return correlationMatrixMapper.findAll();
    }

    @Override
    public List<CorrelationSummary> getCorrelationSummary() {
        return correlationSummaryMapper.findAll();
    }
}