package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.SalesRatingCorrelation;
import java.util.List;

@Mapper
public interface SalesRatingCorrelationMapper {
    @Select("SELECT monthly_sales, rating, shop_name, category FROM sales_rating_correlation ORDER BY monthly_sales")
    List<SalesRatingCorrelation> findAll();
}
