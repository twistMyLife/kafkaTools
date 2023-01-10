package com.cetc10.server;

import com.cetc10.domain.vo.WebSocketResponse;
import com.cetc10.domain.dto.ProducerWebSocketDto;
import com.cetc10.domain.po.ConnectionInfo;
import com.cetc10.domain.po.ProducerInfo;
import com.cetc10.webSocket.WebsocketProducerSever;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Data
@Slf4j
public class SenderServerImpl {
    ConnectionInfo connectionInfo;
    ProducerInfo producerInfo;

    KafkaProducer<String, String> kafkaProducer;

    //0代表暂停  1代表发送
    int flag = 0;
    List<String> contentList;
    //当前处在文件第几行
    int curContentRow = 0;

    SenderServerImpl(ConnectionInfo connectionInfo, ProducerInfo producerInfo){
        this.connectionInfo = connectionInfo;
        this.producerInfo = producerInfo;
        Properties p = new Properties();
        // kafka地址，多个地址用逗号分割
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionInfo.getKafkaBootstrap());
        // 配置value的序列化类
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 配置key的序列化类
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducer = new KafkaProducer<>(p);

        contentList = Arrays.asList(producerInfo.getLastContent().split("\n"));
        //TODO 加入数据格式校验

        //
        new Thread(()->{
            //计算速率的三个参数
            long timeMillis = System.currentTimeMillis();
            long rowCount = 0;
            long speed = Long.MAX_VALUE;
            long nanoTime = System.nanoTime();

            while(true){
                if(flag == 0){
                    try {
                        Thread.sleep(1000);
                        timeMillis = System.currentTimeMillis();
                        rowCount = 0;
                        speed = Long.MAX_VALUE;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }else if(flag == 1){
                    if(curContentRow >= contentList.size()){
                        if(producerInfo.getIsCycleSend() == 1){
                            curContentRow = 0;
                        }else {
                            kafkaProducer.close();
                            break;
                        }
                    }

                    // 超过100ms采用休眠  低于100ms采用循环比较
                    if(producerInfo.getSendInterval()>100){
                    try {
                        long ms = (long)producerInfo.getSendInterval();
//                        int ns = (int)((producerInfo.getSendInterval()-ms)*1000);
                        Thread.sleep(ms);
//                        Thread.sleep((long)producerInfo.getSendInterval());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        }
                    }else {
                        if(System.nanoTime()-nanoTime<producerInfo.getSendInterval()*1000000)
                            continue;
                        else
                            nanoTime = System.nanoTime();
                    }


                    //发送
                    send();
                    //计算当前发送速率
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
                        WebSocketResponse<ProducerWebSocketDto> response = new WebSocketResponse<>(producerInfo.getId(), "发送正常", new ProducerWebSocketDto(curContentRow, speed,contentList.get(curContentRow - 1)));
                        WebsocketProducerSever.sendToAll(response);
                    }
                    if(curContentRow == contentList.size()-1 && producerInfo.getIsCycleSend() == 0){
                        WebSocketResponse<ProducerWebSocketDto> response = new WebSocketResponse<>(producerInfo.getId(), "发送正常", new ProducerWebSocketDto(curContentRow, speed,contentList.get(curContentRow - 1)));
                        WebsocketProducerSever.sendToAll(response);
                    }

                }
            }
        }).start();
    }

    private void send(){
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(producerInfo.getProducerTopic(),contentList.get(curContentRow++));
        kafkaProducer.send(record);
        log.info("推送成功");
    }

    void continueSend(){
        this.flag = 1;
    }

    void stop(){ this.flag = 0; }

}
