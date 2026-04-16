package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class SalesSummary {
    private String metricName;
    private BigDecimal metricValue;
    private String updateTime;
}