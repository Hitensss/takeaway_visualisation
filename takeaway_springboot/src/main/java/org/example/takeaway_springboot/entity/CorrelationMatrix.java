package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CorrelationMatrix {
    private String variable1;          // 变量1
    private String variable2;          // 变量2
    private BigDecimal correlation;    // 相关系数
    private BigDecimal correlationAbs; // 相关系数绝对值
    private String correlationLevel;   // 相关程度
    private String updateTime;
}
