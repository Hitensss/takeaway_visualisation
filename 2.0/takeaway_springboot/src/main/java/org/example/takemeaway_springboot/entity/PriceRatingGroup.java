package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class PriceRatingGroup {
    private String priceGroup;
    private BigDecimal minPrice;
    private BigDecimal maxPrice;
    private Integer shopCount;
    private BigDecimal avgRating;
    private BigDecimal medianRating;
    private String updateTime;
}
