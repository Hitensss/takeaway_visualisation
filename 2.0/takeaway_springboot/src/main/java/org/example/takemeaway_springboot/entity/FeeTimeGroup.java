package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class FeeTimeGroup {
    private String feeGroup;
    private BigDecimal minFee;
    private BigDecimal maxFee;
    private Integer shopCount;
    private BigDecimal avgTime;
    private Integer medianTime;
    private Integer minTime;
    private Integer maxTime;
    private String updateTime;
}
