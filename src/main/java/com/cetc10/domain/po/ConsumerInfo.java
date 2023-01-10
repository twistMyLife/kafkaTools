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
public class ConsumerInfo {
    @ApiModelProperty(value = "消费者主键ID",required = true)
    int id;
    @ApiModelProperty(value = "消费者对应Connection的id",required = true)
    int recordConnectionInfoId;
    @ApiModelProperty(value = "消费者名",required = true)
    String consumerName;
    @ApiModelProperty(value = "消费者消费的topic",required = true)
    String consumerTopic;
    @ApiModelProperty(value = "消费者组名",required = true)
    String consumerGroupName;
    String comment;
    String timeStamp;
    long offset;
    @ApiModelProperty(value = "是否提交偏移量",required = true)
    int autoCommit;
    @ApiModelProperty(value = "消费间隔",required = true)
    double consumeInterval;
    int isUsed;
    int status;
}
