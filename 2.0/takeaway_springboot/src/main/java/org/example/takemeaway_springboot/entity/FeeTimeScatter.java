package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class FeeTimeScatter {
    private BigDecimal deliveryFee;
    private Integer deliveryTime;
    private String shopName;
    private String category;
    private BigDecimal rating;
    private Integer monthlySales;
    private String updateTime;
}
