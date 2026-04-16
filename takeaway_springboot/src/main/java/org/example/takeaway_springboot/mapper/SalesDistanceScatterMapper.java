package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.SalesDistanceScatter;
import java.util.List;

@Mapper
public interface SalesDistanceScatterMapper {
    @Select("SELECT monthly_sales, distance, shop_name, category, rating FROM sales_distance_scatter ORDER BY distance")
    List<SalesDistanceScatter> findAll();
}
