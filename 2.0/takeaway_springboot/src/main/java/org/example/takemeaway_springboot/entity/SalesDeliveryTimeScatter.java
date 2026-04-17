package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class SalesDeliveryTimeScatter {
    private Integer monthlySales;
    private Integer deliveryTime;
    private String shopName;
    private String category;
    private BigDecimal rating;
    private String updateTime;
}
