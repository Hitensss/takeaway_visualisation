package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryTimeRatingScatter;
import java.util.List;

@Mapper
public interface DeliveryTimeRatingScatterMapper {
    @Select("SELECT delivery_time, rating, shop_name, category FROM delivery_time_rating_scatter ORDER BY delivery_time")
    List<DeliveryTimeRatingScatter> findAll();
}
