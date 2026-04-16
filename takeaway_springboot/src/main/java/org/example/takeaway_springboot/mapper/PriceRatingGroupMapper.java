package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.PriceRatingGroup;
import java.util.List;

@Mapper
public interface PriceRatingGroupMapper {
    @Select("SELECT price_group, min_price, max_price, shop_count, avg_rating, median_rating FROM price_rating_group ")
    List<PriceRatingGroup> findAll();
}