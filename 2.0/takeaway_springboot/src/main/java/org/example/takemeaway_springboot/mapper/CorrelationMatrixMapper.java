package org.example.takeaway_springboot.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.example.takeaway_springboot.entity.CorrelationMatrix;
import java.util.List;

@Mapper
public interface CorrelationMatrixMapper {

    @Select("SELECT variable1, variable2, correlation, correlation_abs, correlation_level " +
            "FROM correlation_matrix WHERE variable1 != variable2 " +
            "ORDER BY correlation_abs DESC")
    List<CorrelationMatrix> findAll();
}
