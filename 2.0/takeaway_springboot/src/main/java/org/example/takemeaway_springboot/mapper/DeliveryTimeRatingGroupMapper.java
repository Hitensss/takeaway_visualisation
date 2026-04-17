package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryTimeRatingGroup;
import java.util.List;

@Mapper
public interface DeliveryTimeRatingGroupMapper {
    @Select("SELECT time_group, min_time, max_time, shop_count, avg_rating, median_rating, high_rating_ratio FROM delivery_time_rating_group ORDER BY avg_rating")
    List<DeliveryTimeRatingGroup> findAll();
}