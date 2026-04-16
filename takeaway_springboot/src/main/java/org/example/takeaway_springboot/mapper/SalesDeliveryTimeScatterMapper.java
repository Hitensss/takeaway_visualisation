package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.SalesDeliveryTimeScatter;
import java.util.List;

@Mapper
public interface SalesDeliveryTimeScatterMapper {
    @Select("SELECT monthly_sales, delivery_time, shop_name, category, rating FROM sales_delivery_time_scatter ORDER BY delivery_time")
    List<SalesDeliveryTimeScatter> findAll();
}