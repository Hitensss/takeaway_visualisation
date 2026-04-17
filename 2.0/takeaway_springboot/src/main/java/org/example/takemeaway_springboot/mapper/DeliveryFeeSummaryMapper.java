package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryFeeSummary;
import java.util.List;

@Mapper
public interface DeliveryFeeSummaryMapper {
    @Select("SELECT metric_name, metric_value FROM delivery_fee_summary")
    List<DeliveryFeeSummary> findAll();
}