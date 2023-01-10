package com.cetc10.Mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cetc10.domain.po.ConsumerInfo;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface ConsumerMapper extends BaseMapper<ConsumerInfo> {

    @Select("SELECT max(id) FROM consumer_info")
    Integer selectMaxId();
    @Select("SELECT id FROM consumer_info WHERE record_connection_info_id = #{record_connection_info_id}")
    List<Integer> selectConnectionId(Integer record_connection_info_id);
}
