package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CategoryRatingStats;
import java.util.List;

@Mapper
public interface CategoryRatingStatsMapper {
    @Select("SELECT category, shop_count, avg_rating, min_rating, max_rating, rating_stddev, high_rating_ratio FROM category_rating_stats ORDER BY avg_rating DESC")
    List<CategoryRatingStats> findAll();
}
