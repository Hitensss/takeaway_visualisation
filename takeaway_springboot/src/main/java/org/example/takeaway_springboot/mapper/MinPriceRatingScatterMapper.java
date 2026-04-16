package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.MinPriceRatingScatter;
import java.util.List;

@Mapper
public interface MinPriceRatingScatterMapper {
    @Select("SELECT min_price, rating, shop_name, category FROM min_price_rating_scatter ORDER BY min_price")
    List<MinPriceRatingScatter> findAll();
}
