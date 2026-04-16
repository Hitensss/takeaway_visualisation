package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CategoryWordcloud {
    private String category;
    private BigDecimal weight;
    private String updateTime;
}
