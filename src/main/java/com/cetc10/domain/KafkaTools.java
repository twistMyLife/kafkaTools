package com.cetc10.domain;

import com.cetc10.Util.KafkaUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaTools {

    public String groupId = "kafkaTools";

    private Map<String, KafkaConsumer<String,String>> kafkaConsumerMap = new HashMap<>();

    //key为生产者的id,value为生产者目前的发送状态
    private Map<Integer,Integer> producerStatusMap = new HashMap<>();

    //key为消费者的id,value为消费者目前的收停状态
    private Map<Integer,Integer> consumerStatusMap = new HashMap<>();


    public KafkaConsumer<String,String> getKafkaConsumerByBootStrap(String bootStrap){
        if(kafkaConsumerMap.containsKey(bootStrap)){
            return kafkaConsumerMap.get(bootStrap);
        }
        else {
            KafkaUtil kafkaUtil = new KafkaUtil();
            KafkaConsumer<String, String> consumer = kafkaUtil.getConsumer(bootStrap, groupId);
            kafkaConsumerMap.put(bootStrap,consumer);
            return consumer;
        }
    }

    public void delKafkaConsumerByBootStrapAndStop(String bootStrap){
        kafkaConsumerMap.get(bootStrap).close();
        kafkaConsumerMap.remove(bootStrap);
    }

    public void setProducerStatus(Integer id , Integer status){
        producerStatusMap.put(id,status);
    }

    public int getProducerStatus(Integer id){
        if(!producerStatusMap.containsKey(id))
            return -1;
        return producerStatusMap.get(id);
    }

    public void setConsumerStatus(Integer id , Integer status){
        consumerStatusMap.put(id,status);
    }

    public int getConsumerStatus(Integer id){
        if(!consumerStatusMap.containsKey(id))
            return -1;
        return consumerStatusMap.get(id);
    }
}
