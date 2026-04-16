package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DeliveryFeeDistribution {
    private String feeBucket;
    private BigDecimal minFee;
    private BigDecimal maxFee;
    private Integer shopCount;
    private BigDecimal percentage;
    private BigDecimal cumulativePercentage;
    private String updateTime;
}
