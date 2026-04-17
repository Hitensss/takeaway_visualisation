package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CategoryStats {
    private String category;      // 品类
    private Integer shopCount;    // 店铺数量
    private BigDecimal avgSales;  // 平均月售
    private BigDecimal avgPrice;  // 平均人均
    private BigDecimal avgRating; // 平均评分
    private String updateTime;    // 更新时间
}