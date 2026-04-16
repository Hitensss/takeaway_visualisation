package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class DeliveryTimeRatingScatter {
    private Integer deliveryTime;
    private BigDecimal rating;
    private String shopName;
    private String category;
    private String updateTime;
}
