package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class SalesRatingCorrelation {
    private Integer monthlySales;
    private BigDecimal rating;
    private String shopName;
    private String category;
    private String updateTime;
}
