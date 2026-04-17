package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CategoryRatingDistribution {
    private String category;
    private String ratingBucket;
    private Integer shopCount;
    private BigDecimal percentage;
    private String updateTime;
}
