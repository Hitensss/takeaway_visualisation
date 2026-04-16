package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DeliveryTimeDistribution {
    private String timeBucket;
    private Integer minTime;
    private Integer maxTime;
    private Integer shopCount;
    private BigDecimal percentage;
    private BigDecimal cumulativePercentage;
    private String updateTime;
}
