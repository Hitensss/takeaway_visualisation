package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.MinPriceRatingGroup;
import java.util.List;

@Mapper
public interface MinPriceRatingGroupMapper {
    @Select("SELECT price_group, min_price, max_price, shop_count, avg_rating, median_rating, high_rating_ratio FROM min_price_rating_group ORDER BY avg_rating")
    List<MinPriceRatingGroup> findAll();
}
