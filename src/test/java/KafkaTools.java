import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

@Slf4j
public class KafkaTools {

    public static Properties getConsumeProperties(String groupID, String bootstrap_server) {

        Properties props = new Properties();

        props.put("group.id", groupID);

        props.put("bootstrap.servers", bootstrap_server);

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;

    }

    public static void main(String[] args) {

        String bootstrap_server = "192.168.182.128:9092";

        String groupID = "test2";

        String topic = "test";

        Map<Integer,Long> endOffsetMap = new HashMap<>();

        Map<Integer,Long> commitOffsetMap = new HashMap<>();

        Properties consumeProps = getConsumeProperties(groupID, bootstrap_server);

        log.info("consumer properties:" + consumeProps);

//查询topic partitions

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumeProps);

        List<TopicPartition> topicPartitions = new ArrayList<>();

        List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);

        for (PartitionInfo partitionInfo : partitionsFor) {

            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());

            topicPartitions.add(topicPartition);

        }

//查询log size

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

        for (TopicPartition partitionInfo : endOffsets.keySet()) {

            endOffsetMap.put(partitionInfo.partition(), endOffsets.get(partitionInfo));

        }

        for (int partitionId : endOffsetMap.keySet()) {

            System.out.printf("at %s, topic:%s, partition:%s, logSize:%s%n", System.currentTimeMillis(), topic, partitionId, endOffsetMap.get(partitionId));

        }

//查询消费offset

        for (TopicPartition topicAndPartition : topicPartitions) {

            OffsetAndMetadata committed = consumer.committed(topicAndPartition);

            commitOffsetMap.put(topicAndPartition.partition(), committed.offset());

        }

//累加lag

        long lagSum = 0L;

        if (endOffsetMap.size() == commitOffsetMap.size()) {

            for (int partition : endOffsetMap.keySet()) {

                long endOffSet = endOffsetMap.get(partition);

                long commitOffSet = commitOffsetMap.get(partition);

                long diffOffset = endOffSet - commitOffSet;

                lagSum += diffOffset;

                log.info("Topic:" + topic + ", groupID:" + groupID + ", partition:" + partition + ", endOffset:" + endOffSet + ", autoCommit:" + commitOffSet + ", diffOffset:" + diffOffset);
            }
            log.info("Topic:" + topic + ", groupID:" + groupID + ", LAG:" + lagSum);

        } else {

            log.info("this topic partitions lost");

        }

        consumer.close();

    }

}