package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CategorySalesStats;
import java.util.List;

@Mapper
public interface CategorySalesStatsMapper {
    @Select("SELECT category, shop_count, total_sales, avg_sales, median_sales, min_sales, max_sales, high_sales_ratio FROM category_sales_stats ORDER BY avg_sales DESC")
    List<CategorySalesStats> findAll();
}