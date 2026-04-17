package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.TopShops;
import java.util.List;

@Mapper
public interface TopShopsMapper {
    @Select("SELECT shop_name, category, monthly_sales, avg_price, rating, distance FROM top_shops ORDER BY monthly_sales DESC")
    List<TopShops> findAll();
}
