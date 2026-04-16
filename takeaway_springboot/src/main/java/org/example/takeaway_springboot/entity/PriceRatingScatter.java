package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class PriceRatingScatter {
    private BigDecimal avgPrice;
    private BigDecimal rating;
    private String shopName;
    private String category;
    private String updateTime;
}
