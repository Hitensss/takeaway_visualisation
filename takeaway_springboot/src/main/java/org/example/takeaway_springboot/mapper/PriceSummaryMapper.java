package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.PriceSummary;
import java.util.List;

@Mapper
public interface PriceSummaryMapper {
    @Select("SELECT metric_name, metric_value FROM price_summary")
    List<PriceSummary> findAll();
}
