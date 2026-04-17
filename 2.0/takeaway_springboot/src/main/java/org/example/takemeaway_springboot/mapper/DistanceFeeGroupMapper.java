package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DistanceFeeGroup;
import java.util.List;

@Mapper
public interface DistanceFeeGroupMapper {
    @Select("SELECT distance_group, min_distance, max_distance, shop_count, avg_fee, median_fee, min_fee, max_fee FROM distance_fee_group ORDER BY min_distance")
    List<DistanceFeeGroup> findAll();
}
