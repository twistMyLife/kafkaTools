package com.cetc10.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConsumerWebSocketDto {
    long logSize;
    long offset;
    long lag;
    long speed;
    String message;
}
