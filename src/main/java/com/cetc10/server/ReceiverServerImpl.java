package com.cetc10.server;

import com.cetc10.Thread.ReceiverServerThread;
import com.cetc10.Util.KafkaUtil;
import com.cetc10.domain.vo.WebSocketResponse;
import com.cetc10.domain.dto.ConsumerWebSocketDto;
import com.cetc10.domain.dto.MyConsumerRecord;
import com.cetc10.domain.po.ConnectionInfo;
import com.cetc10.domain.po.ConsumerInfo;
import com.cetc10.webSocket.WebsocketConsumerSever;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Data
@Slf4j
public class ReceiverServerImpl {
    ConnectionInfo connectionInfo;
    ConsumerInfo consumerInfo;

    KafkaUtil kafkaUtil = new KafkaUtil();
    KafkaConsumer<String,String> kafkaConsumer;
    ReceiverServerThread receiverServerThread;

    long timeMillis ;
    long rowCount ;
    long speed ;
    long nanoTime;


    ReceiverServerImpl(ConnectionInfo connectionInfo, ConsumerInfo consumerInfo){
        timeMillis = System.currentTimeMillis()+1000;
        rowCount = 0;
        speed = Long.MAX_VALUE;
        nanoTime = System.nanoTime();

        this.connectionInfo = connectionInfo;
        this.consumerInfo = consumerInfo;

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionInfo.getKafkaBootstrap());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, consumerInfo.getConsumerGroupName());
        AtomicReference<KafkaConsumer<String, String>> kafkaConsumerAtomicReference = new AtomicReference<>(new KafkaConsumer<>(p));
//        AtomicReference<KafkaConsumer> kafkaConsumer = new KafkaConsumer<String, String>(p);
        kafkaConsumer = kafkaConsumerAtomicReference.get();
        kafkaConsumer.subscribe(Collections.singletonList(consumerInfo.getConsumerTopic()));

        receiverServerThread = new ReceiverServerThread(0, this, consumerInfo);
        receiverServerThread.start();

//        new Thread(()->{
//            while(true){
//                if(flag == 0){
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }else if(flag == 1){
//                    consume(consumerInfo.getConsumerTopic(),consumerInfo.getConsumerGroupName(),
//                            consumerInfo.getAutoCommit() == 1);
//                }
//            }
//        }).start();

    }

    public void consume(String topic, String groupId, boolean autoCommit) {
        List<Map<Integer, Long>> offsetAndLogSize = kafkaUtil.searchOffsetAndLogSize(kafkaConsumer, topic, groupId);
        Map<Integer, Long> commitOffsetMap = offsetAndLogSize.get(0);
        Map<Integer, Long> logSizeMap = offsetAndLogSize.get(1);
        Map<Integer, Long> lagMap = offsetAndLogSize.get(2);
        ConsumerRecords<String, String> records= null;
        //关的时候并发关，可能会触发ConcurrentModificationException
        try{
            records = kafkaConsumer.poll(200);
        }catch (ConcurrentModificationException e){

        }
        if(records != null && records.isEmpty()){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }

        //组装为自己的对象  用于排序
        ArrayList<MyConsumerRecord<String,String>> myConsumerRecords = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            MyConsumerRecord<String, String> myConsumerRecord = new MyConsumerRecord<>();
            myConsumerRecord.setPartition(record.partition());
            myConsumerRecord.setOffset(record.offset());
            myConsumerRecord.setValue(record.value());
            myConsumerRecords.add(myConsumerRecord);
        }
        Collections.sort(myConsumerRecords);


        for (MyConsumerRecord<String, String> record : myConsumerRecords) {
            // 超过100ms采用休眠  低于100ms采用循环比较
            if(consumerInfo.getConsumeInterval()>100){
                try {
                    long ms = (long)consumerInfo.getConsumeInterval();
//                    int ns = (int)((consumerInfo.getConsumeInterval()*1000));
                    Thread.sleep(ms);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                while (System.nanoTime()-nanoTime<consumerInfo.getConsumeInterval()*1000000){
                }
                    nanoTime = System.nanoTime();
            }

            //计算当前消费速率
            rowCount++;
            long curTime = System.currentTimeMillis();
            long minusTime = curTime - timeMillis;
            if(minusTime >1000){
                speed = rowCount/((minusTime)/1000);
                timeMillis = curTime;
                rowCount = 0;
            }
            //webSocket推送发送完毕消息
            //如果速率小于10则每一条都websocket推 如果大于10，则每秒推一次
            if(speed < 10 || (speed > 10 && minusTime >1000)){
                long logSize = logSizeMap.get(record.getPartition());
                long offset = record.getOffset()+1;
                WebSocketResponse<ConsumerWebSocketDto> response = new WebSocketResponse<>(consumerInfo.getId(), "消费正常",
                        new ConsumerWebSocketDto(logSize, offset, Math.max(logSize-offset,0),speed,record.getValue()));
                WebsocketConsumerSever.sendToAll(response);
            }

        }


        if(autoCommit){
            while(true){
                try {
                    kafkaConsumer.commitAsync();
                }catch (ConcurrentModificationException e){
                    continue;
                }catch (IllegalStateException e){
                    break;
                }
                break;
            }
        }


    }

    void continueSend(){
        receiverServerThread.flag = 1;
    }

    void stop(){
        receiverServerThread.flag = 0;
    }

    public void close(){
        stop();
        while (true){
            try{
                kafkaConsumer.close();
            }catch (ConcurrentModificationException e){
                continue;
            }
            break;
        }
        receiverServerThread.stop();
    }

    public int getStatus(){
        return receiverServerThread.flag;
    }

}
