package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CategoryRatingStats {
    private String category;
    private Integer shopCount;
    private BigDecimal avgRating;
    private BigDecimal minRating;
    private BigDecimal maxRating;
    private BigDecimal ratingStddev;
    private BigDecimal highRatingRatio;
    private String updateTime;
}
