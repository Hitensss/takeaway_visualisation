package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CategorySalesDistribution {
    private String category;
    private String salesBucket;
    private Integer shopCount;
    private BigDecimal percentage;
    private String updateTime;
}
