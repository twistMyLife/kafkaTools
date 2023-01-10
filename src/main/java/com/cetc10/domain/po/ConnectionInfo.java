package com.cetc10.domain.po;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConnectionInfo {
    @ApiModelProperty(value = "连接主键ID",required = true)
    int id;
    @ApiModelProperty(value = "kafka名",required = true)
    String kafkaName;
    String kafkaVersion;
    @ApiModelProperty(value = "kafka地址",required = true)
    String kafkaBootstrap;
    String zookeeperBootstrap;
}
