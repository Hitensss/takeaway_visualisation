package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DistanceRatingBoxplot;
import java.util.List;

@Mapper
public interface DistanceRatingBoxplotMapper {
    @Select("SELECT distance_group, min_rating, q1_rating, median_rating, q3_rating, max_rating, mean_rating, shop_count FROM distance_rating_boxplot")
    List<DistanceRatingBoxplot> findAll();
}
