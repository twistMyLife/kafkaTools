package com.cetc10.server;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.cetc10.Mapper.ConnectionInfoMapper;
import com.cetc10.Mapper.ConsumerMapper;
import com.cetc10.Util.KafkaUtil;
import com.cetc10.domain.KafkaTools;
import com.cetc10.domain.dto.OffsetLogsizeLag;
import com.cetc10.domain.po.ConnectionInfo;
import com.cetc10.domain.po.ConsumerInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class ConsumerServerImpl {

    @Autowired
    ConnectionServerImpl connectionServer;
    @Autowired
    ConnectionInfoMapper connectionInfoMapper;
    @Autowired
    ConsumerMapper consumerMapper;
    @Autowired
    KafkaTools kafkaTools;

    private HashMap<Integer, ReceiverServerImpl> consumerIdAndInfoMap = new HashMap<>();
    //value存储最近一次暂停的时间,如果时间超过5秒,则真正退出group
    private HashMap<Integer,Long> consumerIdAndTimerThread = new HashMap<>();

    public void newConsumer(int recordConnectionInfoId,
                            String consumerName,
                            String consumerTopic,
                            String consumerGroupName,
                            String comment,
                            int commitOffset,
                            int status
                            ){
        List<ConsumerInfo> consumerInfos = selectConsumerByNameAndConnectionId(consumerName, recordConnectionInfoId);
        if(consumerInfos.size() != 0)
            throw new RuntimeException("消费者名重复");
        Integer i = consumerMapper.selectMaxId();
        ConsumerInfo consumerInfo = ConsumerInfo.builder().recordConnectionInfoId(recordConnectionInfoId).consumerName(consumerName).consumerTopic(consumerTopic)
                .consumerGroupName(consumerGroupName).comment(comment).autoCommit(commitOffset).status(status).build();
        if(i == null)
            consumerInfo.setId(1);
        else
            consumerInfo.setId(i+1);
        consumerMapper.insert(consumerInfo);
    }

    public List<ConsumerInfo> selectAllConsumer(int recordConnectionInfoId){
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<ConsumerInfo> consumerInfoQueryWrapper = new QueryWrapper<>();
        consumerInfoQueryWrapper.eq("record_connection_info_id",recordConnectionInfoId).orderByAsc("consumer_name");
        List<ConsumerInfo> consumerInfos = consumerMapper.selectList(consumerInfoQueryWrapper);
        checkConsumerStatus(consumerInfos);
        return consumerInfos;
    }

    public int deleteConsumer(int id){
        return consumerMapper.deleteById(id);
    }


    public int updateConsumer(ConsumerInfo consumerInfo){
        List<ConsumerInfo> consumerInfos = selectConsumerByNameAndConnectionId(consumerInfo.getConsumerName(),consumerInfo.getRecordConnectionInfoId());
        if(consumerInfos.size() != 0 && consumerInfos.get(0).getId() != consumerInfo.getId())
            throw new RuntimeException("消费者名重复");
        receive(consumerInfo,0);
        consumerInfo.setIsUsed(0);
        return consumerMapper.updateById(consumerInfo);
    }


    public List<ConsumerInfo> fuzzySelectConsumerByNameAndConnectionId(String fuzzyString, int recordConnectionInfoId){
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<ConsumerInfo> consumerInfoQueryWrapper = new QueryWrapper<>();
        consumerInfoQueryWrapper.like("consumer_name",fuzzyString).eq("record_connection_info_id",recordConnectionInfoId);
        List<ConsumerInfo> consumerInfos = consumerMapper.selectList(consumerInfoQueryWrapper);
        checkConsumerStatus(consumerInfos);
        return consumerInfos;
    }


    public List<ConsumerInfo> selectConsumerByNameAndConnectionId(String name, int recordConnectionInfoId){
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<ConsumerInfo> consumerInfoQueryWrapper = new QueryWrapper<>();
        consumerInfoQueryWrapper.eq("consumer_name",name).eq("record_connection_info_id",recordConnectionInfoId);
        return consumerMapper.selectList(consumerInfoQueryWrapper);
    }

    //将数据库中查出来的status变为内存中的status
    public void checkConsumerStatus(List<ConsumerInfo> consumerInfos){
        for (ConsumerInfo consumerInfo : consumerInfos) {
            int status;
            if(!consumerIdAndInfoMap.containsKey(consumerInfo.getId()))
                status = 0;
            else
                status = consumerIdAndInfoMap.get(consumerInfo.getId()).getStatus();
//            int status = kafkaTools.getConsumerStatus(consumerInfo.getId());
//            if(status == -1)
//                consumerInfo.setStatus(0);
//            else
            consumerInfo.setStatus(status);
        }
    }

    public void consumerConsume(int consumerId, int consumeAndStopFlag){
        ConsumerInfo consumerInfo = consumerMapper.selectById(consumerId);
        if(consumerInfo == null)
            throw new RuntimeException("不存在此consumerId");
        receive(consumerInfo,consumeAndStopFlag);

    }

    public void receive(ConsumerInfo consumerInfo, int consumeAndStopFlag){

//        "0代表暂停消费，1代表开始消费
        int consumerId = consumerInfo.getId();
        ConnectionInfo connectionInfo = connectionInfoMapper.selectById(consumerInfo.getRecordConnectionInfoId());
        ReceiverServerImpl receiverServer;

        if(!consumerIdAndInfoMap.keySet().contains(consumerId)){
            //第一次启停，创建对应对象
            receiverServer = new ReceiverServerImpl(connectionInfo,consumerInfo);
            consumerIdAndInfoMap.put(consumerId,receiverServer);
            consumerInfo.setIsUsed(1);
        }else {
            if(consumerInfo.getIsUsed() == 0){
                consumerIdAndInfoMap.get(consumerId).close();

                //历史存在过此消费者，但是用户更改了数据库配置，则清空原缓存
                receiverServer = new ReceiverServerImpl(connectionInfo,consumerInfo);
                consumerIdAndInfoMap.put(consumerId,receiverServer);
                consumerInfo.setIsUsed(1);
            }else if(consumerInfo.getIsUsed() == 1){
                //历史存在过此发送对象，则继续使用
                receiverServer = consumerIdAndInfoMap.get(consumerId);
            }
            else{
                throw new RuntimeException("IsUsed参数异常");
            }
        }

        consumerInfo.setStatus(consumeAndStopFlag);
        consumerMapper.updateById(consumerInfo);
        kafkaTools.setConsumerStatus(consumerId,consumeAndStopFlag);

        if(consumeAndStopFlag == 0){
            //TODO 懒退出 从目前测试的情况看  rebalance速度还可以,暂时不需要懒退出
//            new Thread(()->{
//
//
//            });
//            receiverServer.stop();
            receiverServer.close();
            consumerIdAndInfoMap.remove(consumerId);
        }else if(consumeAndStopFlag == 1){
            receiverServer.continueSend();
        }else if(consumeAndStopFlag == 2)
            receiverServer.close();

    }

    public void setConsumerOffset(int consumerId ,long offset){
        ConsumerInfo consumerInfo = consumerMapper.selectById(consumerId);
        ConnectionInfo connectionInfo = connectionInfoMapper.selectById(consumerInfo.getRecordConnectionInfoId());
        KafkaUtil kafkaUtil = new KafkaUtil();

        //停掉消费者组消费
        for (Integer id : consumerIdAndInfoMap.keySet()) {
            if(consumerIdAndInfoMap.get(id).consumerInfo.getConsumerGroupName().equals(consumerInfo.getConsumerGroupName())){
                consumerIdAndInfoMap.get(id).close();
                consumerIdAndInfoMap.remove(id);
            }
        }

        KafkaConsumer<String, String> consumer = kafkaUtil.getConsumer(connectionInfo.getKafkaBootstrap(), consumerInfo.getConsumerGroupName());
        kafkaUtil.setKafkaOffSet(consumer,consumerInfo.getConsumerTopic(),offset);
        consumer.close();

        //重建停掉的消费者
        for (Integer id : consumerIdAndInfoMap.keySet()) {
            if (consumerIdAndInfoMap.get(id).consumerInfo.getConsumerGroupName().equals(consumerInfo.getConsumerGroupName())) {
                ReceiverServerImpl receiverServer = new ReceiverServerImpl(connectionInfo, consumerInfo);
                consumerIdAndInfoMap.put(id, receiverServer);
                consumerInfo.setIsUsed(1);
                kafkaTools.setConsumerStatus(consumerId,0);
            }
        }
        //offSet入库
        consumerInfo.setOffset(offset);
        consumerMapper.updateById(consumerInfo);
    }

    public OffsetLogsizeLag selectOffsetLogSizeLag(int consumerId){
        ConsumerInfo consumerInfo = consumerMapper.selectById(consumerId);
        ConnectionInfo connectionInfo = connectionInfoMapper.selectById(consumerInfo.getRecordConnectionInfoId());
        KafkaUtil kafkaUtil = new KafkaUtil();
        KafkaConsumer<String, String> consumer = kafkaUtil.getConsumer(connectionInfo.getKafkaBootstrap(),consumerInfo.getConsumerGroupName());
        List<Map<Integer, Long>> offsetLogSizeLag = kafkaUtil.searchOffsetAndLogSize(consumer, consumerInfo.getConsumerTopic(), consumerInfo.getConsumerGroupName());
        Map<Integer, Long> offset = offsetLogSizeLag.get(0);
        Map<Integer, Long> logSize = offsetLogSizeLag.get(1);
        Map<Integer, Long> lag = offsetLogSizeLag.get(2);

        int partirionNum = offset.keySet().size();
        long offsetResult = 0;
        long logSizeResult = 0;
        long lagResult = 0;
        for (int i : offset.keySet()){
         offsetResult += offset.get(i);
         logSizeResult += logSize.get(i);
         lagResult += lag.get(i);
        }
        OffsetLogsizeLag offsetLogsizeLag = new OffsetLogsizeLag(offsetResult/partirionNum,
                logSizeResult/partirionNum,lagResult/partirionNum);
        consumer.close();
        return offsetLogsizeLag;
    }

    public int getMaxConsumerId(){
        Integer maxId = consumerMapper.selectMaxId();
        return maxId == null ? 0 : maxId;
    }
}
