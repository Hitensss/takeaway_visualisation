package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CategoryRatingBoxplot;
import java.util.List;

@Mapper
public interface CategoryRatingBoxplotMapper {
    @Select("SELECT category, shop_count, min_rating, q1_rating, median_rating, q3_rating, max_rating, mean_rating FROM category_rating_boxplot ORDER BY median_rating DESC")
    List<CategoryRatingBoxplot> findAll();
}
