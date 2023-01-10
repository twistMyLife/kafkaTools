import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;

@Slf4j
public class Producer {

    public static String topic = "gqwTest1";// 定义主题
    public static void main(String[] args) throws InterruptedException {
        Properties p = new Properties();
        // kafka地址，多个地址用逗号分割
//        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.182.128:9092");
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.50.37:9091,192.168.50.37:9092,192.168.50.37:9093");
        // 配置value的序列化类
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 配置key的序列化类
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 0表示不确认主服务器是否收到消息,马上返回,低延迟但最弱的持久性,数据可能会丢失
        // 1表示确认主服务器收到消息后才返回,持久性稍强,可是如果主服务器死掉,从服务器数据尚未同步,数据可能会丢失
        // -1表示确认所有服务器都收到数据,完美!
        p.put("request.required.acks", "-1");
        // 异步生产,批量存入缓存后再发到服务器去
        p.put("producer.type", "async");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(p);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "测试消息");
            kafkaProducer.send(record);
            log.info("推送成功");
        } finally {
            // 必须关闭
            kafkaProducer.close();
        }
    }

}