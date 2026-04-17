package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DistanceRatingBoxplot {
    private String distanceGroup;
    private BigDecimal minRating;
    private BigDecimal q1Rating;
    private BigDecimal medianRating;
    private BigDecimal q3Rating;
    private BigDecimal maxRating;
    private BigDecimal meanRating;
    private Integer shopCount;
    private String updateTime;
}
