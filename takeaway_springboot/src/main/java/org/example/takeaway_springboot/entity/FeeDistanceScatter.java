package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class FeeDistanceScatter {
    private BigDecimal deliveryFee;
    private Integer distance;
    private String shopName;
    private String category;
    private BigDecimal rating;
    private Integer monthlySales;
    private String updateTime;
}