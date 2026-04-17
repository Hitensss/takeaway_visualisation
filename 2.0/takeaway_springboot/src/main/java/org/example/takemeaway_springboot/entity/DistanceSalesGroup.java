package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DistanceSalesGroup {
    private String distanceGroup;
    private Integer minDistance;
    private Integer maxDistance;
    private Integer shopCount;
    private BigDecimal avgSales;
    private Integer medianSales;
    private Integer totalSales;
    private String updateTime;
}
