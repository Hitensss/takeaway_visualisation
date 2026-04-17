package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CategoryDistanceBoxplot;
import java.util.List;

@Mapper
public interface CategoryDistanceBoxplotMapper {

    @Select("SELECT category, shop_count, min_distance, q1_distance, " +
            "median_distance, q3_distance, max_distance, avg_distance, " +
            "stddev_distance FROM category_distance_boxplot " +
            "ORDER BY median_distance DESC")
    List<CategoryDistanceBoxplot> findAll();
}
