package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.PriceRatingScatter;
import java.util.List;

@Mapper
public interface PriceRatingScatterMapper {
    @Select("SELECT avg_price, rating, shop_name, category FROM price_rating_scatter ORDER BY avg_price")
    List<PriceRatingScatter> findAll();
}
