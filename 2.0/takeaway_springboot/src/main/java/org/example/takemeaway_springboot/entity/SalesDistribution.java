package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class SalesDistribution {
    private String salesBucket;
    private Integer minSales;
    private Integer maxSales;
    private Integer shopCount;
    private BigDecimal percentage;
    private BigDecimal cumulativePercentage;
    private String updateTime;
}
