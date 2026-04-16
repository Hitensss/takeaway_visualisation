package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CategoryDistanceBoxplot {
    private String category;           // 品类
    private Integer shopCount;         // 店铺数量
    private Integer minDistance;       // 最小距离
    private Integer q1Distance;        // 第一四分位数
    private Integer medianDistance;    // 中位数
    private Integer q3Distance;        // 第三四分位数
    private Integer maxDistance;       // 最大距离
    private BigDecimal avgDistance;    // 平均距离
    private BigDecimal stddevDistance; // 标准差
    private String updateTime;
}
