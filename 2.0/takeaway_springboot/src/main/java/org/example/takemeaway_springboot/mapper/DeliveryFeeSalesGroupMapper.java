package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryFeeSalesGroup;
import java.util.List;

@Mapper
public interface DeliveryFeeSalesGroupMapper {
    @Select("SELECT fee_group, min_fee, max_fee, shop_count, avg_sales, median_sales, total_sales FROM delivery_fee_sales_group ORDER BY avg_sales")
    List<DeliveryFeeSalesGroup> findAll();
}
