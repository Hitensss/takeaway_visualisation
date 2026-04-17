package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DeliveryFeeRatingScatter;
import java.util.List;

@Mapper
public interface DeliveryFeeRatingScatterMapper {
    @Select("SELECT delivery_fee, rating, shop_name, category FROM delivery_fee_rating_scatter ORDER BY delivery_fee")
    List<DeliveryFeeRatingScatter> findAll();
}
