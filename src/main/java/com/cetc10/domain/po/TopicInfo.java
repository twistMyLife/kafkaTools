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
public class TopicInfo {
    @ApiModelProperty(value = "TopicID",required = true)
    int id;
    @ApiModelProperty(value = "Topic对应Connection的id",required = true)
    int recordConnectionInfoId;
    @ApiModelProperty(value = "Topic名",required = true)
    String topicName;
    @ApiModelProperty(value = "分区数量",required = true)
    int partitions;
    @ApiModelProperty(value = "副本数量",required = true)
    int replicas;
}
