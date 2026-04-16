package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryTimeSummary;
import java.util.List;

@Mapper
public interface DeliveryTimeSummaryMapper {
    @Select("SELECT metric_name, metric_value FROM delivery_time_summary")
    List<DeliveryTimeSummary> findAll();
}
