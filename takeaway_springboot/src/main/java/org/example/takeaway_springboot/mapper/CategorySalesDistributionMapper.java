package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CategorySalesDistribution;
import java.util.List;

@Mapper
public interface CategorySalesDistributionMapper {
    @Select("SELECT category, sales_bucket, shop_count, percentage FROM category_sales_distribution ORDER BY category, sales_bucket")
    List<CategorySalesDistribution> findAll();
}