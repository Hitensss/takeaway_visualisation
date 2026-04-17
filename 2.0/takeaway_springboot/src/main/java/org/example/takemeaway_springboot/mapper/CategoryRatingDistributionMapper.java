package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CategoryRatingDistribution;
import java.util.List;

@Mapper
public interface CategoryRatingDistributionMapper {
    @Select("SELECT category, rating_bucket, shop_count, percentage FROM category_rating_distribution ORDER BY category, rating_bucket")
    List<CategoryRatingDistribution> findAll();
}
