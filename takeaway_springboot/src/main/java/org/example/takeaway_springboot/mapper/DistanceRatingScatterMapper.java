package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.DistanceRatingScatter;
import java.util.List;

@Mapper
public interface DistanceRatingScatterMapper {
    @Select("SELECT distance, rating, shop_name, category FROM distance_rating_scatter ORDER BY distance")
    List<DistanceRatingScatter> findAll();
}
