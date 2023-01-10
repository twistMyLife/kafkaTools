package com.cetc10.Mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cetc10.domain.po.ConnectionInfo;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

@Component
public interface ConnectionInfoMapper extends BaseMapper<ConnectionInfo> {

    @Select("SELECT max(id) FROM connection_info")
    Integer selectMaxId();

}
