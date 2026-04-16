package org.example.takeaway_springboot.service;

import org.example.takeaway_springboot.entity.*;

import java.util.List;

/**
 * 外卖配送分析服务接口
 */
public interface DeliveryAnalysisService {

    // ==================== 1. 配送距离分析 ====================

    /**
     * 获取距离区间统计
     */
    List<DistanceStats> getDistanceStats();

    /**
     * 获取品类距离箱线图数据
     */
    List<CategoryDistanceBoxplot> getCategoryDistanceBoxplot();

    // ==================== 2. 送达时间分析 ====================

    /**
     * 获取送达时间分布
     */
    List<DeliveryTimeDistribution> getDeliveryTimeDistribution();

    /**
     * 获取送达时间统计摘要
     */
    List<DeliveryTimeSummary> getDeliveryTimeSummary();

    // ==================== 3. 配送费分析 ====================

    /**
     * 获取配送费分布
     */
    List<DeliveryFeeDistribution> getDeliveryFeeDistribution();

    /**
     * 获取配送费统计摘要
     */
    List<DeliveryFeeSummary> getDeliveryFeeSummary();

    /**
     * 获取配送费-距离散点图数据
     */
    List<FeeDistanceScatter> getFeeDistanceScatter();

    /**
     * 获取距离分组配送费统计
     */
    List<DistanceFeeGroup> getDistanceFeeGroup();

    /**
     * 获取配送费-送达时间散点图数据
     */
    List<FeeTimeScatter> getFeeTimeScatter();

    /**
     * 获取配送费分组送达时间统计
     */
    List<FeeTimeGroup> getFeeTimeGroup();

    // ==================== 4. 相关性分析 ====================

    /**
     * 获取相关系数矩阵
     */
    List<CorrelationMatrix> getCorrelationMatrix();

    /**
     * 获取相关性统计摘要
     */
    List<CorrelationSummary> getCorrelationSummary();
}