package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CategorySalesStats {
    private String category;
    private Integer shopCount;
    private Integer totalSales;
    private BigDecimal avgSales;
    private Integer medianSales;
    private Integer minSales;
    private Integer maxSales;
    private BigDecimal highSalesRatio;
    private String updateTime;
}
