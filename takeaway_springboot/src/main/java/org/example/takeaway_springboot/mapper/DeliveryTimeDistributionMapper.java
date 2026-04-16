package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryTimeDistribution;
import java.util.List;

@Mapper
public interface DeliveryTimeDistributionMapper {
    @Select("SELECT time_bucket, min_time, max_time, shop_count, percentage, cumulative_percentage FROM delivery_time_distribution ")
    List<DeliveryTimeDistribution> findAll();
}
