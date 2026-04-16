package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.SalesDeliveryFeeScatter;
import java.util.List;

@Mapper
public interface SalesDeliveryFeeScatterMapper {
    @Select("SELECT monthly_sales, delivery_fee, shop_name, category, rating FROM sales_delivery_fee_scatter ORDER BY delivery_fee")
    List<SalesDeliveryFeeScatter> findAll();
}
