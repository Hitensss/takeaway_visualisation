package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DeliveryTimeSalesGroup {
    private String timeGroup;
    private Integer minTime;
    private Integer maxTime;
    private Integer shopCount;
    private BigDecimal avgSales;
    private Integer medianSales;
    private Integer totalSales;
    private String updateTime;
}
