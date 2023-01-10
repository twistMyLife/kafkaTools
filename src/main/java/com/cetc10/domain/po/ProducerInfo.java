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
public class ProducerInfo {
     @ApiModelProperty(value = "生产者主键ID",required = true)
     int id;
     @ApiModelProperty(value = "生产者对应Connection的id",required = true)
     int recordConnectionInfoId;
     @ApiModelProperty(value = "生产者名",required = true)
     String producerName;
     @ApiModelProperty(value = "生产者生产的到的topic",required = true)
     String producerTopic;
     String comment;
     String lastContent;
     String contentType;
     int isCycleSend;
     int isRandomAppend;
     double sendInterval;
     int isUsed;
     int status;


}
