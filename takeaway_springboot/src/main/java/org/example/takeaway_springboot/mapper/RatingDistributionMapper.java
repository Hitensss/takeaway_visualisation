package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.RatingDistribution;
import java.util.List;

@Mapper
public interface RatingDistributionMapper {
    @Select("SELECT rating_bucket, shop_count, percentage FROM rating_distribution ORDER BY percentage")
    List<RatingDistribution> findAll();
}
