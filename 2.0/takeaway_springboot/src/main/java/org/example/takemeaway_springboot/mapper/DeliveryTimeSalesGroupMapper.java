package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryTimeSalesGroup;
import java.util.List;

@Mapper
public interface DeliveryTimeSalesGroupMapper {
    @Select("SELECT time_group, min_time, max_time, shop_count, avg_sales, median_sales, total_sales FROM delivery_time_sales_group ORDER BY avg_sales")
    List<DeliveryTimeSalesGroup> findAll();
}