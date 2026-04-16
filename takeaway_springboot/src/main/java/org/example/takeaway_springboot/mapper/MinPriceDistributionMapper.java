package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.MinPriceDistribution;
import java.util.List;

@Mapper
public interface MinPriceDistributionMapper {
    @Select("SELECT price_bucket, min_price, max_price, shop_count, percentage, cumulative_percentage FROM min_price_distribution ORDER BY percentage")
    List<MinPriceDistribution> findAll();
}
