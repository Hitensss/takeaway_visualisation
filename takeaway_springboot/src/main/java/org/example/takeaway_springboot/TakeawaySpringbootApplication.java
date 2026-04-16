package org.example.takeaway_springboot;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("org.example.takeaway_springboot.mapper")
public class TakeawaySpringbootApplication {

    public static void main(String[] args) {
        SpringApplication.run(TakeawaySpringbootApplication.class, args);
        System.out.println("========================================");
        System.out.println("  外卖数据分析系统启动成功！");
        System.out.println("  访问地址: http://localhost:8081");
        System.out.println("========================================");
    }
}

