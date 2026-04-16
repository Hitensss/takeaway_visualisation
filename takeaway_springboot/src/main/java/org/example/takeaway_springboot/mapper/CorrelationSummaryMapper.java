package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CorrelationSummary;
import java.util.List;

@Mapper
public interface CorrelationSummaryMapper {
    @Select("SELECT variable_pair, correlation, relationship_type, insight FROM correlation_summary ORDER BY ABS(correlation) DESC")
    List<CorrelationSummary> findAll();
}
