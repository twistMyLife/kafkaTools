package com.cetc10.Util;

import com.cetc10.Exception.LogExpiredException;
import com.cetc10.domain.KafkaTools;
import com.cetc10.domain.dto.MessageResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class KafkaUtil {


    public String connectKafka(String bootstrap_server,String groupID,String topic) throws InterruptedException {

        AtomicReference<Map<String, Boolean>> bootStrapMap = new AtomicReference<>(new ConcurrentHashMap<>());

        if(bootstrap_server.contains(",")){
            String[] bootStraps = bootstrap_server.split(",");
            CountDownLatch countDownLatch = new CountDownLatch(bootStraps.length);
            for (String bootStrap : bootStraps) {
                new Thread(()->{
                    boolean b = testConnection(bootstrap_server, groupID);
                    if(b)
                        bootStrapMap.get().put(bootStrap,b);
                    countDownLatch.countDown();
                }).start();
            }
            countDownLatch.await();

            for (String s : bootStrapMap.get().keySet()) {
                if(bootStrapMap.get().get(s) != null){
                    try {
                        DescribeClusterResult describeClusterResult = testConnection(bootstrap_server);
                        assert describeClusterResult != null;
                        return "kafka所有节点:"+describeClusterResult.nodes().get().toString() + "      kafka Controller节点:"+describeClusterResult.controller().get().toString();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
            throw  new RuntimeException("连接超时");

        }else {
            boolean b = testConnection(bootstrap_server, groupID);
            if (!b)
                return "bootStrap:" + bootstrap_server + "连接超时";
            else {
                try {
                    DescribeClusterResult describeClusterResult = testConnection(bootstrap_server);
                    assert describeClusterResult != null;
                    return "kafka所有节点:"+describeClusterResult.nodes().get().toString() + "     kafka Controller节点:"+describeClusterResult.controller().get().toString();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }

        return null;

    }


//    class ConnectionThread extends Thread{
//        AtomicReference<DescribeClusterResult> atomicReference;
//        AdminClient kafkaAdminClient;
//        @Override
//        public void run() {
//            atomicReference.set(kafkaAdminClient.describeCluster());
//        }
//    }

    //通过kafkaAdmin测试链接
    private DescribeClusterResult testConnection(String bootstrap_server) {
        AdminClient kafkaAdminClient = getKafkaAdminClient(bootstrap_server);
        AtomicReference<DescribeClusterResult> atomicReference = new AtomicReference<>();
//        ConnectionThread connectionThread = new ConnectionThread();
//        connectionThread.kafkaAdminClient = kafkaAdminClient;
//        connectionThread.atomicReference = atomicReference;
//        connectionThread.start();

        new Thread(() -> {
            atomicReference.set(kafkaAdminClient.describeCluster());
        }).start();

        int time = 0;
        while (time <= 5) {

            if (atomicReference.get() != null && atomicReference.get().nodes().isDone())
                return atomicReference.get();
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            time++;

        }
        return null;
    }

    //通过消费者订阅测试链接
    private boolean testConnection(String bootstrap_server, String groupID) {
        KafkaConsumer consumer = getConsumer(bootstrap_server, groupID);
        final Map[] atomicMap = {new ConcurrentHashMap()};

        new Thread(()->{
            atomicMap[0] = consumer.listTopics();
        }).start();

        int time = 0;
        while (time <= 5) {

            if (atomicMap[0] .size() != 0)
                return true;
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            time++;

        }
        return false;
    }


    public AdminClient getKafkaAdminClient(String bootstrap_server){
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,50000);
//        p.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG,50000);
//        p.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,50000);
//        p.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG,50000);
//        p.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,50000);
        return AdminClient.create(p);
    }

    public KafkaConsumer<String,String> getConsumer(String bootstrap_server,String groupID){
        // kafka设置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap_server);
        properties.put("group.id", groupID);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "6000");
        properties.put("session.timeout.ms", "10000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(properties);
    }

    public List<MessageResponseDto> consumeAsList(KafkaTools kafkaTools,KafkaConsumer<String,String> kafkaConsumer,String bootstrap,  String topic, String groupId ,boolean autuCommit) throws LogExpiredException {
        ArrayList<MessageResponseDto> resultList = new ArrayList<>();

        List<Map<Integer, Long>> offsetAndLogSize = searchOffsetAndLogSize(kafkaConsumer, topic, groupId);
        Map<Integer, Long> curOffset = offsetAndLogSize.get(0);
        Map<Integer, Long> logSize = offsetAndLogSize.get(1);
//        KafkaUtil kafkaUtil = new KafkaUtil();
//        kafkaConsumer = kafkaUtil.getConsumer(bootstrap,groupId);
//        kafkaConsumer = kafkaTools.getKafkaConsumerByBootStrap(bootstrap);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
//        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
//        for (int partition : logSize.keySet())
//            topicPartitions.add(new TopicPartition(topic,partition));
//        kafkaConsumer.assign(topicPartitions);
//        //获取多个partition中
//        long maxLogSize = Long.MIN_VALUE;
//        for (Integer key : logSize.keySet())
//            maxLogSize = Math.max(maxLogSize,logSize.get(key));

        //如果拉取数据的offset==logSize的次数达到partition的数量,则代表已经拉取完毕
        long cur = System.currentTimeMillis();
        while (true ) {
            int count = 0;
            for (Integer key : logSize.keySet()){
                if(curOffset.get(key) >= logSize.get(key))
                    count++;
            }
            if(count == logSize.size())
                break;
            if(System.currentTimeMillis()-cur > 4000)
                throw new LogExpiredException("数据存储可能已超期,被kafka删除");
            // 100 是超时时间（ms），在该时间内 poll 会等待服务器返回数据
            ConsumerRecords<String, String> records = kafkaConsumer.poll(200);
            if(records.isEmpty()){
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
            cur = System.currentTimeMillis();
            // poll 返回一个记录列表。
            // 每条记录都包含了记录所属主题的信息、记录所在分区的信息、记录在分区里的偏移量，以及记录的键值对。
            for (ConsumerRecord<String, String> record : records) {
                curOffset.put(record.partition(),record.offset()+1);
                resultList.add(new MessageResponseDto(record.partition(),record.offset(),record.value()));
//                resultList.add("partition: "+ record.partition()+"           offset：" + record.offset()+"                内容：" + record.value());
            }
            if(!records.isEmpty()){
                try {
                    // poll 的数据全部处理完提交
                    if(autuCommit)
                        kafkaConsumer.commitAsync();
                } catch (CommitFailedException e) {
                    e.printStackTrace();
                }
            }
        }
//        kafkaTools.delKafkaConsumerByBootStrapAndStop(bootstrap);
        Collections.sort(resultList);
        return resultList;
    }


    public void setKafkaOffSet(KafkaConsumer<String,String> kafkaConsumer,String topic,long offset) {

        List<TopicPartition> topicPartitions = new ArrayList<>();

        kafkaConsumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
        List<PartitionInfo> partitionsFor = kafkaConsumer.partitionsFor(topic);

        for (PartitionInfo partitionInfo : partitionsFor) {

            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());

            topicPartitions.add(topicPartition);
        }

        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, "manual");
            offsets.put(topicPartition, offsetAndMetadata);
        }

        // 提交
        kafkaConsumer.commitSync(offsets);
        log.info("设置kafkaOffSet成功");
    }

    public void setKafkaOffSet(KafkaConsumer<String,String> kafkaConsumer,String topic, Map<Integer,Long> partitionAndOffset){
        List<TopicPartition> topicPartitions = new ArrayList<>();

        kafkaConsumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
        List<PartitionInfo> partitionsFor = kafkaConsumer.partitionsFor(topic);

        for (PartitionInfo partitionInfo : partitionsFor) {

            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());

            topicPartitions.add(topicPartition);
        }

        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            if(partitionAndOffset.containsKey(topicPartition.partition())){
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(partitionAndOffset.get(topicPartition.partition()), "manual");
                offsets.put(topicPartition, offsetAndMetadata);
            }
        }

        // 提交
        kafkaConsumer.commitSync(offsets);
        log.info("设置kafkaOffSet成功");
    }

    //传入需要设置的时间戳 所有partition设置一样的时间戳
    public void setKafkaTimestamp(KafkaConsumer<String,String> kafkaConsumer, String topicName, long timestamp){
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = queryOffsetForTimestamp(kafkaConsumer, topicName, timestamp);
        //key是partition，value是offset
        HashMap<Integer, Long> partitionAndOffsetMap = new HashMap<>();
        //将timeStamp转换为offset
        for (TopicPartition topicPartition : topicPartitionOffsetAndTimestampMap.keySet()) {
            partitionAndOffsetMap.put(topicPartition.partition(),topicPartitionOffsetAndTimestampMap.get(topicPartition).offset());
        }
        setKafkaOffSet(kafkaConsumer,topicName,partitionAndOffsetMap);

    }

    //传入需要设置的时间戳Map，key为partition，value为timestamp
    public void setKafkaTimestamp(KafkaConsumer<String,String> kafkaConsumer, String topicName, Map<Integer,Long> partitionAndTimestamp){
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = queryOffsetForTimestamp(kafkaConsumer, topicName, partitionAndTimestamp);
        //key是partition，value是offset
        HashMap<Integer, Long> partitionAndOffsetMap = new HashMap<>();
        //将timeStamp转换为offset
        for (TopicPartition topicPartition : topicPartitionOffsetAndTimestampMap.keySet()) {
            partitionAndOffsetMap.put(topicPartition.partition(),topicPartitionOffsetAndTimestampMap.get(topicPartition).offset());
        }
        setKafkaOffSet(kafkaConsumer,topicName,partitionAndOffsetMap);



    }


    public  Map<TopicPartition,OffsetAndTimestamp> queryOffsetForTimestamp(KafkaConsumer<String,String> kafkaConsumer, String topicName, long timestamp ) {
        Map<TopicPartition, OffsetAndTimestamp> pMap;
        List<TopicPartition> tpList = new ArrayList<>();
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topicName);
        for( PartitionInfo partitionInfo : partitionInfoList ){
            tpList.add(new TopicPartition(topicName, partitionInfo.partition()));
        }
        Map<TopicPartition,Long> partitionsQueryMap = new HashMap<>();
        tpList.forEach(tinfo -> {
            partitionsQueryMap.put(tinfo, timestamp);
        });
        pMap = kafkaConsumer.offsetsForTimes(partitionsQueryMap);
        return pMap;
    }

    public  Map<TopicPartition,OffsetAndTimestamp> queryOffsetForTimestamp(KafkaConsumer<String,String> kafkaConsumer, String topicName, Map<Integer,Long> partitionAndTimestamp ) {
        Map<TopicPartition, OffsetAndTimestamp> pMap;
        List<TopicPartition> tpList = new ArrayList<>();
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topicName);
        for( PartitionInfo partitionInfo : partitionInfoList ){
            tpList.add(new TopicPartition(topicName, partitionInfo.partition()));
        }
        Map<TopicPartition,Long> partitionsQueryMap = new HashMap<>();
        tpList.forEach(tinfo -> {
            if(partitionAndTimestamp.containsKey(tinfo.partition()))
                partitionsQueryMap.put(tinfo, partitionAndTimestamp.get(tinfo.partition()));
        });
        pMap = kafkaConsumer.offsetsForTimes(partitionsQueryMap);
        return pMap;
    }

    public List<Map<Integer,Long>> searchOffsetAndLogSize(KafkaConsumer<String,String> kafkaConsumer,String topicName,String groupID)  {
        Map<Integer,Long> offsetMap = new HashMap<>();

        Map<Integer,Long> logSizeMap = new HashMap<>();

        Map<Integer,Long> lagMap = new HashMap<>();

        List<TopicPartition> topicPartitions = new ArrayList<>();

        List<PartitionInfo> partitionsFor = kafkaConsumer.partitionsFor(topicName);

        for (PartitionInfo partitionInfo : partitionsFor) {

            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());

            topicPartitions.add(topicPartition);

        }

//查询log size

        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions);

        for (TopicPartition partitionInfo : endOffsets.keySet()) {

            logSizeMap.put(partitionInfo.partition(), endOffsets.get(partitionInfo));

        }

        for (int partitionId : logSizeMap.keySet()) {

//            System.out.printf("at %s, topic:%s, partition:%s, logSize:%s%n", System.currentTimeMillis(), topicName, partitionId, endOffsetMap.get(partitionId));

        }

//查询消费offset

        for (TopicPartition topicAndPartition : topicPartitions) {


            OffsetAndMetadata committed = kafkaConsumer.committed(topicAndPartition);

            if(committed == null){
                offsetMap.put(topicAndPartition.partition(),logSizeMap.get(topicAndPartition.partition()));
//                throw new CommitOffsetNotFoundException("新建加入kafka的组没有offset信息");
            }
            else
            offsetMap.put(topicAndPartition.partition(), committed.offset());

        }

//累加lag

        long lagSum = 0L;

        if (logSizeMap.size() == offsetMap.size()) {

            for (int partition : logSizeMap.keySet()) {

                long endOffSet = logSizeMap.get(partition);

                long commitOffSet = offsetMap.get(partition);

                long diffOffset = endOffSet - commitOffSet;

                lagSum += diffOffset;

                log.info("Topic:" + topicName + ", groupID:" + groupID + ", partition:" +
                        partition + ", endOffset:" + endOffSet + ", autoCommit:" + commitOffSet + ", diffOffset:" + diffOffset);
                lagMap.put(partition,diffOffset);
            }


        } else {
            log.error("this topic partitions lost");

        }

        ArrayList<Map<Integer, Long>> offsetLogSizeLag = new ArrayList<>();
        offsetLogSizeLag.add(offsetMap);
        offsetLogSizeLag.add(logSizeMap);
        offsetLogSizeLag.add(lagMap);
        return offsetLogSizeLag;

    }

    public int getBrokerNum(String bootStrap) {
        DescribeClusterResult describeClusterResult = testConnection(bootStrap);
        try {
            assert describeClusterResult != null;
            return describeClusterResult.nodes().get().size();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }


}
