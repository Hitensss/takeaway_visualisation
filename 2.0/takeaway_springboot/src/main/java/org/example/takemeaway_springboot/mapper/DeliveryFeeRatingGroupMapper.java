package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryFeeRatingGroup;
import java.util.List;

@Mapper
public interface DeliveryFeeRatingGroupMapper {
    @Select("SELECT fee_group, min_fee, max_fee, shop_count, avg_rating, median_rating, high_rating_ratio FROM delivery_fee_rating_group ORDER BY avg_rating")
    List<DeliveryFeeRatingGroup> findAll();
}
