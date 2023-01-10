import com.cetc10.Util.KafkaUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

/**
 * 消费者
 *
 * @author AdminMall
 *
 */
public class Consumer {
    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.50.37:9091");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 自动提交秒
        // p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, false);
        // 是否开启自动提交
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // group 代表一个消费组,加入组里面,消息只能被该组的一个消费者消费
        // 如果所有消费者在一个组内,就是传统的队列模式,排队拿消息
        // 如果所有的消费者都不在同一个组内,就是发布-订阅模式,消息广播给所有组
        // 如果介于两者之间,那么广播的消息在组内也是要排队的
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);
        // 订阅消息；subscribe()
        // 方法接受一个主题列表作为参数：也可以接收一个正则表达式，匹配多个主题；consumer.subscribe("test.*");
        kafkaConsumer.subscribe(Collections.singletonList(Producer.topic));
        while (true) {
            // 100 是超时时间（ms），在该时间内 poll 会等待服务器返回数据
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            Map<String, List<PartitionInfo>> stringListMap = kafkaConsumer.listTopics();

            // poll 返回一个记录列表。
            // 每条记录都包含了记录所属主题的信息、记录所在分区的信息、记录在分区里的偏移量，以及记录的键值对。
            for (ConsumerRecord<String, String> record : records) {
                // 主题
                System.out.println("主题：" + record.topic());
                System.out.println("读取位置：" + record.offset());
                System.out.println("Key：" + record.key());
                System.out.println("内容：" + record.value());
            }
            try {
                // poll 的数据全部处理完提交
                kafkaConsumer.commitAsync();
            } catch (CommitFailedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void Consume(){
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.50.37:9091,192.168.50.37:9092,192.168.50.37:9093");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 自动提交秒
        // p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, false);
        // 是否开启自动提交
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // group 代表一个消费组,加入组里面,消息只能被该组的一个消费者消费
        // 如果所有消费者在一个组内,就是传统的队列模式,排队拿消息
        // 如果所有的消费者都不在同一个组内,就是发布-订阅模式,消息广播给所有组
        // 如果介于两者之间,那么广播的消息在组内也是要排队的
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);
        // 订阅消息；subscribe()
        // 方法接受一个主题列表作为参数：也可以接收一个正则表达式，匹配多个主题；consumer.subscribe("test.*");
        kafkaConsumer.subscribe(Collections.singletonList("gqwTest1"));



        while (true) {
            // 100 是超时时间（ms），在该时间内 poll 会等待服务器返回数据
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

            // poll 返回一个记录列表。
            // 每条记录都包含了记录所属主题的信息、记录所在分区的信息、记录在分区里的偏移量，以及记录的键值对。
            for (ConsumerRecord<String, String> record : records) {
                // 主题
                System.out.println("主题：" + record.topic());
                System.out.println("读取位置：" + record.offset());
                System.out.println("Key：" + record.key());
                System.out.println("内容：" + record.value());
            }
            try {
                // poll 的数据全部处理完提交
                kafkaConsumer.commitAsync();
            } catch (CommitFailedException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void resetOffset(){
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.50.37:9091,192.168.50.37:9092,192.168.50.37:9093");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 自动提交秒
        // p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, false);
        // 是否开启自动提交
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,10000);
        // group 代表一个消费组,加入组里面,消息只能被该组的一个消费者消费
        // 如果所有消费者在一个组内,就是传统的队列模式,排队拿消息
        // 如果所有的消费者都不在同一个组内,就是发布-订阅模式,消息广播给所有组
        // 如果介于两者之间,那么广播的消息在组内也是要排队的
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaTools");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);
        // 订阅消息；subscribe()
        // 方法接受一个主题列表作为参数：也可以接收一个正则表达式，匹配多个主题；consumer.subscribe("test.*");
        kafkaConsumer.subscribe(Collections.singletonList("gqwTest1"));

        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

        List<TopicPartition> topicPartitions = new ArrayList<>();
        List<PartitionInfo> partitionsFor = kafkaConsumer.partitionsFor("gqwTest1");
        for (PartitionInfo partitionInfo : partitionsFor) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        }
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0,"");
            offsets.put(topicPartition, offsetAndMetadata);
        }
        // 提交
        kafkaConsumer.commitSync(offsets);
        System.out.println("设置kafkaOffSet成功");
    }

    @Test
    public void selectOffset(){
        String groupId = "myGroup2";
        String topicName = "gqwTest1";
        KafkaUtil kafkaUtil = new KafkaUtil();
        KafkaConsumer<String,String> consumer = kafkaUtil.getConsumer("192.168.50.37:9091,192.168.50.37:9092,192.168.50.37:9093",groupId);
        consumer.subscribe(Collections.singletonList(topicName));
//        KafkaConsumer<String,String> consumer = kafkaUtil.getConsumer(connectionInfo.getKafkaBootstrap(), groupId);
        List<Map<Integer, Long>> offsetAndLogSize = kafkaUtil.searchOffsetAndLogSize(consumer, topicName,groupId);

    }

    @Test
    public void selectTopics(){
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.50.37:9091");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 自动提交秒
        // p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, false);
        // 是否开启自动提交
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // group 代表一个消费组,加入组里面,消息只能被该组的一个消费者消费
        // 如果所有消费者在一个组内,就是传统的队列模式,排队拿消息
        // 如果所有的消费者都不在同一个组内,就是发布-订阅模式,消息广播给所有组
        // 如果介于两者之间,那么广播的消息在组内也是要排队的
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup3");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);
        Map<String, List<PartitionInfo>> stringListMap = kafkaConsumer.listTopics();
        for(String key : stringListMap.keySet()){
            System.out.println("key :" + key);
            System.out.println(stringListMap.get(key));
        }
    }
}
