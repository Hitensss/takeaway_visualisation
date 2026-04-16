package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CategoryStats;
import java.util.List;

@Mapper
public interface CategoryStatsMapper {

    @Select("SELECT category, shop_count, avg_sales, avg_price, avg_rating FROM category_stats ORDER BY avg_sales DESC")
    List<CategoryStats> findAll();

    @Select("SELECT category, shop_count, avg_sales, avg_price, avg_rating FROM category_stats WHERE category = #{category}")
    CategoryStats findByCategory(String category);
}
