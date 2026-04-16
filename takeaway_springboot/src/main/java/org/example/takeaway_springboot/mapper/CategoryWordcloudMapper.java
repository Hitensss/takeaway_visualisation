package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CategoryWordcloud;
import java.util.List;

@Mapper
public interface CategoryWordcloudMapper {
    @Select("SELECT category, weight FROM category_wordcloud ORDER BY weight DESC")
    List<CategoryWordcloud> findAll();
}
