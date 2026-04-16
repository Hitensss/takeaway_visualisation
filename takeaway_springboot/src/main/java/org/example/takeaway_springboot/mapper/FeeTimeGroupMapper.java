package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.FeeTimeGroup;
import java.util.List;

@Mapper
public interface FeeTimeGroupMapper {
    @Select("SELECT fee_group, min_fee, max_fee, shop_count, avg_time, median_time, min_time, max_time FROM fee_time_group ORDER BY avg_time")
    List<FeeTimeGroup> findAll();
}
