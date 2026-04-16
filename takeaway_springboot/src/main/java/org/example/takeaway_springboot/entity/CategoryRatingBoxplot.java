package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CategoryRatingBoxplot {
    private String category;
    private Integer shopCount;
    private BigDecimal minRating;
    private BigDecimal q1Rating;
    private BigDecimal medianRating;
    private BigDecimal q3Rating;
    private BigDecimal maxRating;
    private BigDecimal meanRating;
    private String updateTime;
}
