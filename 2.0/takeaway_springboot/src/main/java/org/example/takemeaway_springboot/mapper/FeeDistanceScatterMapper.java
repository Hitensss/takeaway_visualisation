package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.FeeDistanceScatter;
import java.util.List;

@Mapper
public interface FeeDistanceScatterMapper {
    @Select("SELECT delivery_fee, distance, shop_name, category, rating, monthly_sales FROM fee_distance_scatter ORDER BY distance")
    List<FeeDistanceScatter> findAll();
}
