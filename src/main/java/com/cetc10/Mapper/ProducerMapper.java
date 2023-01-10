package com.cetc10.Mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cetc10.domain.po.ProducerInfo;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface ProducerMapper extends BaseMapper<ProducerInfo> {

    @Select("SELECT max(id) FROM producer_info")
    Integer selectMaxId();

    @Select("SELECT id FROM producer_info WHERE record_connection_info_id = #{record_connection_info_id}")
    List<Integer> selectConnectionId(Integer record_connection_info_id);
}
