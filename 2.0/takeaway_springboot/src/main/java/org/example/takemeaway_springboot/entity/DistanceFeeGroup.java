package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DistanceFeeGroup {
    private String distanceGroup;
    private Integer minDistance;
    private Integer maxDistance;
    private Integer shopCount;
    private BigDecimal avgFee;
    private BigDecimal medianFee;
    private BigDecimal minFee;
    private BigDecimal maxFee;
    private String updateTime;
}
