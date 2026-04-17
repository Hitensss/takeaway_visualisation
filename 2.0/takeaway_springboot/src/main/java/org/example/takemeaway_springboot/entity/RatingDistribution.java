package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class RatingDistribution {
    private String ratingBucket;
    private Integer shopCount;
    private BigDecimal percentage;
    private String updateTime;
}