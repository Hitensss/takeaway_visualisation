package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class MinPriceDistribution {
    private String priceBucket;
    private BigDecimal minPrice;
    private BigDecimal maxPrice;
    private Integer shopCount;
    private BigDecimal percentage;
    private BigDecimal cumulativePercentage;
    private String updateTime;
}
