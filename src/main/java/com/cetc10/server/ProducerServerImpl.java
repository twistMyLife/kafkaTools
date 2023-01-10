package com.cetc10.server;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.cetc10.Mapper.ConnectionInfoMapper;
import com.cetc10.Mapper.ProducerMapper;
import com.cetc10.domain.KafkaTools;
import com.cetc10.domain.po.ConnectionInfo;
import com.cetc10.domain.po.ProducerInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ProducerServerImpl {

    @Autowired
    ProducerMapper producerMapper;
    @Autowired
    ConnectionInfoMapper connectionInfoMapper;
    @Autowired
    ConnectionServerImpl connectionServer;
    @Autowired
    KafkaTools kafkaTools;

    private Map<Integer,SenderServerImpl> producerIdAndInfoMap = new ConcurrentHashMap<>();

    public void newProducer( int recordConnectionInfoId,
                             String producerName,
                             String producerTopic,
                             String comment,
                             String lastContent,
                             String contentType,
                             Integer isCycleSend,
                             Integer isRandomAppend,
                             Double sendInterval){
        if(isCycleSend == null)
            isCycleSend = 0;
        if(isRandomAppend == null)
            isRandomAppend = 0;
        if(sendInterval == null)
            sendInterval = 100D;
        List<ProducerInfo> producerInfos = selectProducerByNameAndConnectionId(producerName, recordConnectionInfoId);
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        if(producerInfos.size() != 0)
            throw new RuntimeException("生产者名重复");
        Integer i = producerMapper.selectMaxId();
        ProducerInfo producerInfo = ProducerInfo.builder().recordConnectionInfoId(recordConnectionInfoId).producerName(producerName).producerTopic(producerTopic)
                .comment(comment).lastContent(lastContent).contentType(contentType).isCycleSend(isCycleSend).isRandomAppend(isRandomAppend).sendInterval(sendInterval).isUsed(0).build();

        if(i == null)
            producerInfo.setId(1);
        else
            producerInfo.setId(i+1);
        producerMapper.insert(producerInfo);

    }

    public List<ProducerInfo> selectAllProducer(int recordConnectionInfoId){
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<ProducerInfo> producerInfoQueryWrapper = new QueryWrapper<>();
        producerInfoQueryWrapper.eq("record_connection_info_id",recordConnectionInfoId).orderByAsc("producer_name");
        List<ProducerInfo> producerInfos = producerMapper.selectList(producerInfoQueryWrapper);
        checkProducerStatus(producerInfos);
        return producerInfos;
    }

    public int deleteProducer(int id){
        return producerMapper.deleteById(id);
    }

    public int updateProducer(ProducerInfo producerInfo){
        connectionServer.isSelectConnectionIdExist(producerInfo.getRecordConnectionInfoId());
        List<ProducerInfo> producerInfos = selectProducerByNameAndConnectionId(producerInfo.getProducerName(), producerInfo.getRecordConnectionInfoId());
        if(producerInfos.size() != 0 && producerInfos.get(0).getId() != producerInfo.getId())
            throw new RuntimeException("生产者名重复");
        send(producerInfo,0);
        producerInfo.setIsUsed(0);
        return producerMapper.updateById(producerInfo);
    }


    public List<ProducerInfo> fuzzySelectProducerByNameAndConnectionId(String fuzzyString, int recordConnectionInfoId){
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<ProducerInfo> producerInfoQueryWrapper = new QueryWrapper<>();
        producerInfoQueryWrapper.like("producer_name",fuzzyString).eq("record_connection_info_id",recordConnectionInfoId);
        List<ProducerInfo> producerInfos = producerMapper.selectList(producerInfoQueryWrapper);
        checkProducerStatus(producerInfos);
        return producerInfos;
    }

    public List<ProducerInfo> selectProducerByNameAndConnectionId(String name, int recordConnectionInfoId){
        connectionServer.isSelectConnectionIdExist(recordConnectionInfoId);
        QueryWrapper<ProducerInfo> producerInfoQueryWrapper = new QueryWrapper<>();
        producerInfoQueryWrapper.eq("producer_name",name).eq("record_connection_info_id",recordConnectionInfoId);
        return producerMapper.selectList(producerInfoQueryWrapper);
    }

    public void checkProducerStatus(List<ProducerInfo> producerInfos){
        for (ProducerInfo producerInfo : producerInfos) {
            int status = kafkaTools.getProducerStatus(producerInfo.getId());
            if(status == -1)
                producerInfo.setStatus(0);
            else
                producerInfo.setStatus(status);
        }
    }

    public void producerSend(int producerId, int sendAndStopFlag){
        ProducerInfo producerInfo = producerMapper.selectById(producerId);
        if(producerInfo == null)
            throw new RuntimeException("不存在此producerId");
        send(producerInfo,sendAndStopFlag);

    }

    public void send(ProducerInfo producerInfo ,int sendAndStopFlag){
//        "0代表暂停发送，1代表开始发送，2代表重发
        ConnectionInfo connectionInfo = connectionInfoMapper.selectById(producerInfo.getRecordConnectionInfoId());
        SenderServerImpl senderServer;
        int producerId = producerInfo.getId();
        if(!producerIdAndInfoMap.keySet().contains(producerId)){
            //第一次启停，创建对应对象
            senderServer = new SenderServerImpl(connectionInfo,producerInfo);
            producerIdAndInfoMap.put(producerId,senderServer);
            producerInfo.setIsUsed(1);
        }else {
            if(producerInfo.getIsUsed() == 0){
                producerIdAndInfoMap.get(producerId).stop();
                //历史存在过此发送对象，但是用户更改了数据库配置，则清空原缓存
                senderServer = new SenderServerImpl(connectionInfo,producerInfo);
                producerIdAndInfoMap.remove(producerId);
                producerIdAndInfoMap.put(producerId,senderServer);
                producerInfo.setIsUsed(1);
            }else if(producerInfo.getIsUsed() == 1){
                //历史存在过此发送对象，则继续使用
                senderServer = producerIdAndInfoMap.get(producerId);
            }
            else{
                throw new RuntimeException("IsUsed参数异常");
            }

        }
        producerInfo.setStatus(sendAndStopFlag);
        //设置内存中的Status
        kafkaTools.setProducerStatus(producerId,sendAndStopFlag);
        producerMapper.updateById(producerInfo);

        if(sendAndStopFlag == 0){
            senderServer.stop();
        }else if(sendAndStopFlag == 1){
            senderServer.continueSend();
        }else if(sendAndStopFlag == 2){
            senderServer.stop();
            producerIdAndInfoMap.remove(producerId);
            //数据库更新状态为1
            send(producerInfo,0);
        }

    }

    public int getMaxProducerId(){
        Integer maxId = producerMapper.selectMaxId();
        return maxId == null ? 0 : maxId;
    }
}
