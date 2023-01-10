package com.cetc10.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.util.Optional;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MyConsumerRecord<K,V> implements Comparable<MyConsumerRecord> {
    private  String topic;
    private  int partition;
    private  long offset;
    private  long timestamp;
    private  TimestampType timestampType;
    private  int serializedKeySize;
    private  int serializedValueSize;
    private  Headers headers;
    private  K key;
    private  V value;
    private  Optional<Integer> leaderEpoch;

    private volatile Long checksum;

    @Override
    public int compareTo(MyConsumerRecord o) {
        return Long.compare(this.offset,o.offset);
    }
}
