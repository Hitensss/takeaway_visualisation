package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CorrelationSummary {
    private String variablePair;
    private BigDecimal correlation;
    private String relationshipType;
    private String insight;
    private String updateTime;
}
