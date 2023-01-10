package com.cetc10.server;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.cetc10.Mapper.ConnectionInfoMapper;
import com.cetc10.Mapper.ConsumerMapper;
import com.cetc10.Mapper.ProducerMapper;
import com.cetc10.Mapper.TopicMapper;
import com.cetc10.Util.KafkaUtil;
import com.cetc10.domain.po.ConnectionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class ConnectionServerImpl {

    @Autowired
    ConnectionInfoMapper connectionInfoMapper;
    @Autowired
    ProducerMapper producerMapper;
    @Autowired
    ConsumerMapper consumerMapper;
    @Autowired
    TopicMapper topicMapper;


    public String testConnection(
                                String kafkaVersion,
                                String kafkaBootStrap) throws InterruptedException {
        KafkaUtil kafkaUtil = new KafkaUtil();
        String con = null;
        con = kafkaUtil.connectKafka(kafkaBootStrap, "kafkaTool", "kafkaTool");

        return con;
    }

    public void addConnection(String kafkaName,String kafkaVersion,String kafkaBootStrap,String zookeeperBootStrap){
        List<ConnectionInfo> connectionInfos = selectConnectionByKafkaName(kafkaName);
        if(connectionInfos.size() != 0)
            throw new RuntimeException("kafka名重复");
        Integer i = connectionInfoMapper.selectMaxId();
        ConnectionInfo connectionInfo = ConnectionInfo.builder().kafkaName(kafkaName).kafkaVersion(kafkaVersion).kafkaBootstrap(kafkaBootStrap).
                zookeeperBootstrap(zookeeperBootStrap).build();
        if(i == null)
            connectionInfo.setId(1);
        else
            connectionInfo.setId(i+1);

        connectionInfoMapper.insert(connectionInfo);

    }

        public List<ConnectionInfo> selectAllConnection(){
            QueryWrapper<ConnectionInfo> connectionInfoQueryWrapper = new QueryWrapper<>();
            connectionInfoQueryWrapper.orderByAsc("kafka_name");
            return connectionInfoMapper.selectList(connectionInfoQueryWrapper);
        }
//    public List<ConnectionData> selectAllConnection(){
//        List<ConnectionInfo> connectionInfos = connectionInfoMapper.selectList(null);
//        KafkaUtil kafkaUtil = new KafkaUtil();
//
//        ArrayList<ConnectionData> connectionDatas = new ArrayList<>();
//        for (ConnectionInfo connectionInfo : connectionInfos){
//            String kafkaData = kafkaUtil.getKafkaData(connectionInfo.getKafkaBootstrap());
//            connectionDatas.add(new ConnectionData(connectionInfo,kafkaData));
//        }
//        return connectionDatas;
//    }

    @Transactional
    public int deleteConnection(int id){
        //删除连接 删除对应消费者 生产者 topic
        int result = 0;
        result |= connectionInfoMapper.deleteById(id);
        result |= topicMapper.deleteBatchIds(topicMapper.selectConnectionId(id));
        result |= consumerMapper.deleteBatchIds(consumerMapper.selectConnectionId(id));
        result |= producerMapper.deleteBatchIds(producerMapper.selectConnectionId(id));
        return  result;
    }

    public int updateConnection(ConnectionInfo connectionInfo){
        List<ConnectionInfo> connectionInfos = selectConnectionByKafkaName(connectionInfo.getKafkaName());
        if(connectionInfos.size() != 0)
            throw new RuntimeException("kafka名重复");
        return connectionInfoMapper.updateById(connectionInfo);
    }

    public List<ConnectionInfo> fuzzySelectConnectionByKafkaName(String fuzzyString){
        QueryWrapper<ConnectionInfo> connectionInfoQueryWrapper = new QueryWrapper<>();
        connectionInfoQueryWrapper.like("kafka_name",fuzzyString);
        return connectionInfoMapper.selectList(connectionInfoQueryWrapper);
    }

    public List<ConnectionInfo> selectConnectionByKafkaName(String name){
        QueryWrapper<ConnectionInfo> connectionInfoQueryWrapper = new QueryWrapper<>();
        connectionInfoQueryWrapper.eq("kafka_name",name);
        return connectionInfoMapper.selectList(connectionInfoQueryWrapper);
    }

    public void isSelectConnectionIdExist(int recordConnectionInfoId){
        ConnectionInfo connectionInfo = connectionInfoMapper.selectById(recordConnectionInfoId);
        if(connectionInfo == null)
            throw new RuntimeException("recordConnectionInfoId错误，不存在此连接");
    }
}
