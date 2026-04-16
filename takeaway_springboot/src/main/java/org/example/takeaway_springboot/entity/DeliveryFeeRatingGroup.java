package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DeliveryFeeRatingGroup {
    private String feeGroup;
    private BigDecimal minFee;
    private BigDecimal maxFee;
    private Integer shopCount;
    private BigDecimal avgRating;
    private BigDecimal medianRating;
    private BigDecimal highRatingRatio;
    private String updateTime;
}
