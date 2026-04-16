package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class SalesDeliveryFeeScatter {
    private Integer monthlySales;
    private BigDecimal deliveryFee;
    private String shopName;
    private String category;
    private BigDecimal rating;
    private String updateTime;
}
