package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryFeeDistribution;
import java.util.List;

@Mapper
public interface DeliveryFeeDistributionMapper {
    @Select("SELECT fee_bucket, min_fee, max_fee, shop_count, percentage, cumulative_percentage FROM delivery_fee_distribution ")
    List<DeliveryFeeDistribution> findAll();
}
