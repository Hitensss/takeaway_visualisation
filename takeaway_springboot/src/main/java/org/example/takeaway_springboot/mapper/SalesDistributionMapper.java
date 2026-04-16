package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.SalesDistribution;
import java.util.List;

@Mapper
public interface SalesDistributionMapper {
    @Select("SELECT sales_bucket, min_sales, max_sales, shop_count, percentage, cumulative_percentage FROM sales_distribution ")
    List<SalesDistribution> findAll();
}
