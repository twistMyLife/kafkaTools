package com.cetc10.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class MessageResponseDto implements Comparable<MessageResponseDto>{
    int partition;
    long offset;
    String message;

    @Override
    public int compareTo(MessageResponseDto o) {
        return Long.compare(o.offset,this.offset);
    }
}
