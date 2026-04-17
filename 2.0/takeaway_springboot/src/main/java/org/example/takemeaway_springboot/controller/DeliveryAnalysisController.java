package org.example.takeaway_springboot.controller;

import org.example.takeaway_springboot.dto.ApiResponse;
import org.example.takeaway_springboot.entity.*;
import org.example.takeaway_springboot.service.DeliveryAnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 外卖配送分析Controller
 * 提供配送距离、送达时间、配送费、相关性等分析数据的API接口
 */
@RestController
@RequestMapping("/api/delivery")
@CrossOrigin(origins = "*")
public class DeliveryAnalysisController {

    @Autowired
    private DeliveryAnalysisService deliveryAnalysisService;

    // ==================== 1. 配送距离分析 ====================

    /**
     * 1.1 距离区间统计（直方图）
     * GET /api/delivery/distance-stats
     */
    @GetMapping("/distance-stats")
    public ApiResponse<List<DistanceStats>> getDistanceStats() {
        try {
            List<DistanceStats> data = deliveryAnalysisService.getDistanceStats();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取距离区间统计失败：" + e.getMessage());
        }
    }

    /**
     * 1.2 品类距离箱线图（各品类距离分布对比）
     * GET /api/delivery/category-distance-boxplot
     */
    @GetMapping("/category-distance-boxplot")
    public ApiResponse<List<CategoryDistanceBoxplot>> getCategoryDistanceBoxplot() {
        try {
            List<CategoryDistanceBoxplot> data = deliveryAnalysisService.getCategoryDistanceBoxplot();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取品类距离箱线图数据失败：" + e.getMessage());
        }
    }

    // ==================== 2. 送达时间分析 ====================

    /**
     * 2.1 送达时间分布（直方图）
     * GET /api/delivery/delivery-time-distribution
     */
    @GetMapping("/delivery-time-distribution")
    public ApiResponse<List<DeliveryTimeDistribution>> getDeliveryTimeDistribution() {
        try {
            List<DeliveryTimeDistribution> data = deliveryAnalysisService.getDeliveryTimeDistribution();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取送达时间分布失败：" + e.getMessage());
        }
    }

    /**
     * 2.2 送达时间统计摘要
     * GET /api/delivery/delivery-time-summary
     */
    @GetMapping("/delivery-time-summary")
    public ApiResponse<List<DeliveryTimeSummary>> getDeliveryTimeSummary() {
        try {
            List<DeliveryTimeSummary> data = deliveryAnalysisService.getDeliveryTimeSummary();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取送达时间摘要失败：" + e.getMessage());
        }
    }

    // ==================== 3. 配送费分析 ====================

    /**
     * 3.1 配送费分布（直方图）
     * GET /api/delivery/delivery-fee-distribution
     */
    @GetMapping("/delivery-fee-distribution")
    public ApiResponse<List<DeliveryFeeDistribution>> getDeliveryFeeDistribution() {
        try {
            List<DeliveryFeeDistribution> data = deliveryAnalysisService.getDeliveryFeeDistribution();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取配送费分布失败：" + e.getMessage());
        }
    }

    /**
     * 3.2 配送费统计摘要
     * GET /api/delivery/delivery-fee-summary
     */
    @GetMapping("/delivery-fee-summary")
    public ApiResponse<List<DeliveryFeeSummary>> getDeliveryFeeSummary() {
        try {
            List<DeliveryFeeSummary> data = deliveryAnalysisService.getDeliveryFeeSummary();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取配送费摘要失败：" + e.getMessage());
        }
    }

    /**
     * 3.3 配送费-距离散点图
     * GET /api/delivery/fee-distance-scatter
     */
    @GetMapping("/fee-distance-scatter")
    public ApiResponse<List<FeeDistanceScatter>> getFeeDistanceScatter() {
        try {
            List<FeeDistanceScatter> data = deliveryAnalysisService.getFeeDistanceScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取配送费-距离散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 3.4 距离分组配送费统计（用于柱状图）
     * GET /api/delivery/distance-fee-group
     */
    @GetMapping("/distance-fee-group")
    public ApiResponse<List<DistanceFeeGroup>> getDistanceFeeGroup() {
        try {
            List<DistanceFeeGroup> data = deliveryAnalysisService.getDistanceFeeGroup();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取距离分组配送费统计失败：" + e.getMessage());
        }
    }

    /**
     * 3.5 配送费-送达时间散点图
     * GET /api/delivery/fee-time-scatter
     */
    @GetMapping("/fee-time-scatter")
    public ApiResponse<List<FeeTimeScatter>> getFeeTimeScatter() {
        try {
            List<FeeTimeScatter> data = deliveryAnalysisService.getFeeTimeScatter();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取配送费-送达时间散点图数据失败：" + e.getMessage());
        }
    }

    /**
     * 3.6 配送费分组送达时间统计（用于柱状图）
     * GET /api/delivery/fee-time-group
     */
    @GetMapping("/fee-time-group")
    public ApiResponse<List<FeeTimeGroup>> getFeeTimeGroup() {
        try {
            List<FeeTimeGroup> data = deliveryAnalysisService.getFeeTimeGroup();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取配送费分组送达时间统计失败：" + e.getMessage());
        }
    }

    // ==================== 4. 相关性分析 ====================

    /**
     * 4.1 相关系数矩阵（热力图）
     * GET /api/delivery/correlation-matrix
     */
    @GetMapping("/correlation-matrix")
    public ApiResponse<List<CorrelationMatrix>> getCorrelationMatrix() {
        try {
            List<CorrelationMatrix> data = deliveryAnalysisService.getCorrelationMatrix();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取相关系数矩阵失败：" + e.getMessage());
        }
    }

    /**
     * 4.2 相关性统计摘要
     * GET /api/delivery/correlation-summary
     */
    @GetMapping("/correlation-summary")
    public ApiResponse<List<CorrelationSummary>> getCorrelationSummary() {
        try {
            List<CorrelationSummary> data = deliveryAnalysisService.getCorrelationSummary();
            return ApiResponse.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取相关性摘要失败：" + e.getMessage());
        }
    }
}
