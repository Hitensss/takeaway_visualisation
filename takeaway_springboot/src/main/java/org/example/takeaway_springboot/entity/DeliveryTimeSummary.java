package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DeliveryTimeSummary {
    private String metricName;
    private BigDecimal metricValue;
    private String updateTime;
}
