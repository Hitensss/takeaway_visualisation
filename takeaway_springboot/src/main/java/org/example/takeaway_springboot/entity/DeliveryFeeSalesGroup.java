package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DeliveryFeeSalesGroup {
    private String feeGroup;
    private BigDecimal minFee;
    private BigDecimal maxFee;
    private Integer shopCount;
    private BigDecimal avgSales;
    private Integer medianSales;
    private Integer totalSales;
    private String updateTime;
}

