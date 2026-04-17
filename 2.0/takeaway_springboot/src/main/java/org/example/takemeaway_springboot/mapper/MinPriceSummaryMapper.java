package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.MinPriceSummary;
import java.util.List;

@Mapper
public interface MinPriceSummaryMapper {
    @Select("SELECT metric_name, metric_value FROM min_price_summary")
    List<MinPriceSummary> findAll();
}
