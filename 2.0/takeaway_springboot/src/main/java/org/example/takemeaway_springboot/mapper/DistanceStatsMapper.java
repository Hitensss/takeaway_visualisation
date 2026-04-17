package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DistanceStats;
import java.util.List;

@Mapper
public interface DistanceStatsMapper {
    @Select("SELECT distance_group, shop_count, avg_sales, avg_price, avg_rating FROM distance_stats ORDER BY " +
            "CASE distance_group WHEN '0-300m' THEN 1 WHEN '300-800m' THEN 2 WHEN '800-1500m' THEN 3 ELSE 4 END")
    List<DistanceStats> findAll();
}