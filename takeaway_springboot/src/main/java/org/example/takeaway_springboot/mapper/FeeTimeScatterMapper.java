package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.FeeTimeScatter;
import java.util.List;

@Mapper
public interface FeeTimeScatterMapper {
    @Select("SELECT delivery_fee, delivery_time, shop_name, category, rating, monthly_sales FROM fee_time_scatter ORDER BY delivery_fee")
    List<FeeTimeScatter> findAll();
}
