package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DeliveryTimeRatingGroup {
    private String timeGroup;
    private Integer minTime;
    private Integer maxTime;
    private Integer shopCount;
    private BigDecimal avgRating;
    private BigDecimal medianRating;
    private BigDecimal highRatingRatio;
    private String updateTime;
}
