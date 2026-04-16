package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.SalesSummary;
import java.util.List;

@Mapper
public interface SalesSummaryMapper {
    @Select("SELECT metric_name, metric_value FROM sales_summary")
    List<SalesSummary> findAll();
}
