package org.example.takeaway_springboot.entity;

import lombok.Data;
import java.util.Date;

@Data
public class SparkTaskStatus {
    private Long id;
    private String fileName;        // 文件名
    private String status;          // RUNNING, SUCCESS, FAILED
    private String logOutput;       // 日志输出
    private Date startTime;         // 开始时间
    private Date endTime;           // 结束时间
}