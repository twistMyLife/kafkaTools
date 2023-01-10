package com.cetc10.server;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.cetc10.Exception.LogExpiredException;
import com.cetc10.Exception.ReplicasException;
import com.cetc10.Mapper.ConnectionInfoMapper;
import com.cetc10.Mapper.TopicMapper;
import com.cetc10.Util.KafkaUtil;
import com.cetc10.domain.KafkaTools;
import com.cetc10.domain.dto.MessageResponseDto;
import com.cetc10.domain.po.ConnectionInfo;
import com.cetc10.domain.po.TopicInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class TopicServerImpl {

    @Autowired
    ConnectionInfoMapper connectionInfoMapper;
    @Autowired
    TopicMapper topicMapper;
    @Autowired
    ConnectionServerImpl connectionServer;
    @Autowired
    KafkaTools kafkaTools;

    public void newTopic(int recordConnectionInfoId,String topicName, Integer partitions, Integer replicas){

        //先试着能不能新增  然后入库
        KafkaUtil kafkaUtil = new KafkaUtil();
        ConnectionInfo connectionInfo = connectionInfoMapper.selectById(recordConnectionInfoId);
        //不允许副本数量大于broker数量
        KafkaUtil kafkaUtil1 = new KafkaUtil();
        int brokerNum = kafkaUtil1.getBrokerNum(connectionInfo.getKafkaBootstrap());
        if(replicas > brokerNum)
            throw new ReplicasException("副本数量不允许大于broker数量");
        AdminClient kafkaAdminClient = kafkaUtil.getKafkaAdminClient(connectionInfo.getKafkaBootstrap());
        NewTopic newTopic = new NewTopic(topicName, partitions, (short) (int)replicas);
        kafkaAdminClient.createTopics(Collections.singletonList(newTopic));

        //入库
        insertTopicDb(recordConnectionInfoId,topicName,partitions,replicas);
        //TODO kafka建立topic需要时间 立即查询还没有  让前端等待1秒后再更新列表
    }

    public void insertTopicDb(int recordConnectionInfoId,String topicName, Integer partitions, Integer replicas){
        if(partitions == null)
            partitions = 1;
        if(replicas == null)
            replicas = 1;
        List<TopicInfo> topicInfos = selectTopicByNameAndConnectionId(topicName, recordConnectionInfoId);
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        if(topicInfos.size() != 0)
            throw new RuntimeException("Topic名重复");
        Integer i = topicMapper.selectMaxId();
        TopicInfo topicInfo = TopicInfo.builder().recordConnectionInfoId(recordConnectionInfoId).topicName(topicName).partitions(partitions).replicas(replicas).build();
        if(i == null)
            topicInfo.setId(1);
        else
            topicInfo.setId(i+1);
        topicMapper.insert(topicInfo);
    }

    public List<TopicInfo> selectAllTopic(int recordConnectionInfoId) {
        synKafkaAndDb(recordConnectionInfoId);
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<TopicInfo> topicInfoQueryWrapper = new QueryWrapper<>();
        topicInfoQueryWrapper.eq("record_connection_info_id",recordConnectionInfoId).orderByAsc("topic_name");
        return topicMapper.selectList(topicInfoQueryWrapper);
    }

    /*
        同步kafka和数据库topic表中的数据
        TODO 这个地方设计的有问题，根本不需要存库，下一个版本建议删除
     */
    private void synKafkaAndDb(int recordConnectionInfoId){
        KafkaUtil kafkaUtil = new KafkaUtil();
        ConnectionInfo connectionInfo = connectionInfoMapper.selectById(recordConnectionInfoId);
        KafkaConsumer<String, String> consumer = kafkaUtil.getConsumer(connectionInfo.getKafkaBootstrap(), kafkaTools.groupId);
        Map<String, List<PartitionInfo>> kafkaTopics = consumer.listTopics();

        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<TopicInfo> topicInfoQueryWrapper = new QueryWrapper<>();
        topicInfoQueryWrapper.eq("record_connection_info_id",recordConnectionInfoId);
        List<TopicInfo> dbTopicInfos = topicMapper.selectList(topicInfoQueryWrapper);
        HashSet<String> dbTopicInfosSetName = new HashSet<>();
        for (TopicInfo dbTopicInfo : dbTopicInfos) {
            dbTopicInfosSetName.add(dbTopicInfo.getTopicName());
        }

        for (String topicName : kafkaTopics.keySet()) {
            if(!dbTopicInfosSetName.contains(topicName)){
                insertTopicDb(recordConnectionInfoId,topicName,kafkaTopics.get(topicName).size(),
                        kafkaTopics.get(topicName).get(0).replicas().length);
            }
        }

        for (String topicName : dbTopicInfosSetName) {
            if(!kafkaTopics.containsKey(topicName)){
                List<TopicInfo> topicInfos = selectTopicByNameAndConnectionId(topicName, recordConnectionInfoId);
                for (TopicInfo topicInfo : topicInfos) {
                    topicMapper.deleteById(topicInfo.getId());
                }

            }
        }
    }

    public int deleteTopic(int id) throws ExecutionException, InterruptedException {
        //先试着能不能删除，再删库
        KafkaUtil kafkaUtil = new KafkaUtil();
        TopicInfo topicInfo = topicMapper.selectById(id);
        ConnectionInfo connectionInfo = connectionInfoMapper.selectById(topicInfo.getRecordConnectionInfoId());
        AdminClient kafkaAdminClient = kafkaUtil.getKafkaAdminClient(connectionInfo.getKafkaBootstrap());
        DeleteTopicsResult deleteTopicsResult = kafkaAdminClient.deleteTopics(Collections.singletonList(topicInfo.getTopicName()));
        for (Map.Entry<String, KafkaFuture<Void>> stringKafkaFutureEntry : deleteTopicsResult.values().entrySet()) {
            KafkaFuture<Void> value = stringKafkaFutureEntry.getValue();
            //调用get将会抛出异常
            value.get();
        }
        return topicMapper.deleteById(id);
    }

    public int updateTopic(TopicInfo topicInfo){
        connectionServer.isSelectConnectionIdExist(topicInfo.getRecordConnectionInfoId());
        List<TopicInfo> producerInfos = selectTopicByNameAndConnectionId(topicInfo.getTopicName(), topicInfo.getRecordConnectionInfoId());
        if(producerInfos.size() != 0)
            throw new RuntimeException("生产者名重复");
        return topicMapper.updateById(topicInfo);
    }

    public List<TopicInfo> selectTopicByNameAndConnectionId(String name, int recordConnectionInfoId){
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<TopicInfo> producerInfoQueryWrapper = new QueryWrapper<>();
        producerInfoQueryWrapper.eq("topic_name",name).eq("record_connection_info_id",recordConnectionInfoId);
        return topicMapper.selectList(producerInfoQueryWrapper);
    }

    public List<TopicInfo> fuzzySelectTopicByNameAndConnectionId(String fuzzyString, int recordConnectionInfoId){
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<TopicInfo> producerInfoQueryWrapper = new QueryWrapper<>();
        producerInfoQueryWrapper.like("topic_name",fuzzyString).eq("record_connection_info_id",recordConnectionInfoId);
        return topicMapper.selectList(producerInfoQueryWrapper);
    }

    public List<MessageResponseDto> searchTopicDataById(int id, int num) throws LogExpiredException {
        TopicInfo topicInfo = topicMapper.selectById(id);
        ConnectionInfo connectionInfo = connectionInfoMapper.selectById(topicInfo.getRecordConnectionInfoId());

        KafkaConsumer<String,String> consumer = kafkaTools.getKafkaConsumerByBootStrap(connectionInfo.getKafkaBootstrap());
        KafkaUtil kafkaUtil = new KafkaUtil();
//        KafkaConsumer<String,String> consumer = kafkaUtil.getConsumer(connectionInfo.getKafkaBootstrap(), kafkaTools.groupId);
//        consumer.subscribe(Collections.singletonList(topicInfo.getTopicName()));
        List<Map<Integer, Long>> offsetAndLogSize = kafkaUtil.searchOffsetAndLogSize(consumer, topicInfo.getTopicName(), kafkaTools.groupId);
        Map<Integer, Long> offset = offsetAndLogSize.get(0);
        Map<Integer, Long> logSize = offsetAndLogSize.get(1);
        //根据logSize情况  查看最近n条数据
//        long backToOffset = Long.MAX_VALUE;
//        num = num/logSize.size();
//        for (Integer partition : logSize.keySet()){
//            if(logSize.get(partition) < num){
//                backToOffset = 0;
//                break;
//            }else {
//                backToOffset = Math.min(backToOffset,logSize.get(partition)-num);
//            }
//        }
//        kafkaUtil.setKafkaOffSet(consumer,topicInfo.getTopicName(),backToOffset);

        Map<Integer, Long> shortSlabOffset = shortSlabAlg(logSize, num);
        kafkaUtil.setKafkaOffSet(consumer,topicInfo.getTopicName(),shortSlabOffset);
        kafkaTools.delKafkaConsumerByBootStrapAndStop(connectionInfo.getKafkaBootstrap());
        consumer = kafkaTools.getKafkaConsumerByBootStrap(connectionInfo.getKafkaBootstrap());
        kafkaUtil.searchOffsetAndLogSize(consumer, topicInfo.getTopicName(), kafkaTools.groupId);
        return kafkaUtil.consumeAsList(kafkaTools,consumer,connectionInfo.getKafkaBootstrap(),topicInfo.getTopicName(), kafkaTools.groupId,true);
    }

    //短板效应算法，offset从高到低依次将回退的offset指定完，返回回退后个partition的offset
    public Map<Integer,Long> shortSlabAlg(Map<Integer, Long> logSize,int backNum){
        //以value排序
        ArrayList<Map.Entry<Integer, Long>> list = new ArrayList<>(logSize.entrySet());
        list.sort((o1, o2) -> -o1.getValue().compareTo(o2.getValue()));
        list.add(new HashMap.SimpleEntry<>(-1, 0L));
        for (int i = 0; i < list.size() - 1; i++) {
            long minus = list.get(i).getValue() - list.get(i + 1).getValue();
            long aveBack = Math.min(backNum / (i + 1), minus);
            backNum -= aveBack * (i + 1);
            for (int j = 0; j < i + 1; j++) {
                Integer key = list.get(j).getKey();
                logSize.put(key, logSize.get(key) - aveBack);
            }
        }
        return logSize;
    }
}
