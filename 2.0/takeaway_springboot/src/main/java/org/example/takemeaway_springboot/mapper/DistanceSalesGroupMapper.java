package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DistanceSalesGroup;
import java.util.List;

@Mapper
public interface DistanceSalesGroupMapper {
    @Select("SELECT distance_group, min_distance, max_distance, shop_count, avg_sales, median_sales, total_sales FROM distance_sales_group ORDER BY avg_sales")
    List<DistanceSalesGroup> findAll();
}