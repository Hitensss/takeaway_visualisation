package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class TopShops {
    private String shopName;
    private String category;
    private Integer monthlySales;
    private BigDecimal avgPrice;
    private BigDecimal rating;
    private Integer distance;
    private String updateTime;
}
