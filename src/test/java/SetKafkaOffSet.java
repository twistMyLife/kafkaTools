import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class SetKafkaOffSet {
    public static void main(String[] args) {
        try {

            String bootstrap_server = "192.168.182.128:9092";

            String groupID = "test";


            String topic = "test";

            // kafka设置
            Properties properties = new Properties();
            properties.put("bootstrap.servers", bootstrap_server);
            properties.put("group.id", groupID);
            properties.put("enable.auto.commit", "true");
            properties.put("auto.commit.interval.ms", "6000");
            properties.put("session.timeout.ms", "10000");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("auto.offset.reset", "earliest");
            KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);


            //测试时间戳
            queryOffsetFormTimestamp(consumer,topic,1);

            List<TopicPartition> topicPartitions = new ArrayList<>();

            List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);

            for (PartitionInfo partitionInfo : partitionsFor) {

                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());

                topicPartitions.add(topicPartition);
            }

            HashMap<TopicPartition,OffsetAndMetadata> offsets = new HashMap<>();
            for(TopicPartition topicPartition : topicPartitions){
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(5, "manual");
                offsets.put(topicPartition, offsetAndMetadata);
            }

            // 提交
            consumer.commitSync(offsets);
            System.out.println("ok");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<TopicPartition, OffsetAndTimestamp> queryOffsetFormTimestamp(KafkaConsumer<String,String> kafkaConsumer, String topicName, long timestemp ) {
        Map<TopicPartition, OffsetAndTimestamp> pMap;
        List<TopicPartition> tpList = new ArrayList<>();
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topicName);
        for( PartitionInfo partitionInfo : partitionInfoList ){
            tpList.add(new TopicPartition(topicName, partitionInfo.partition()));
        }
        Map<TopicPartition,Long> partitionsQueryMap = new HashMap<>();
        tpList.forEach(tinfo -> {
            partitionsQueryMap.put(tinfo, timestemp);
        });
        pMap = kafkaConsumer.offsetsForTimes(partitionsQueryMap);
        return pMap;
    }
}