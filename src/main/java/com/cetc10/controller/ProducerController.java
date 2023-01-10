package com.cetc10.controller;


import com.cetc10.domain.vo.HttpResponse;
import com.cetc10.domain.vo.FuzzySelect;
import com.cetc10.domain.po.ProducerInfo;
import com.cetc10.server.ProducerServerImpl;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api("生产者相关接口")
@RestController
@RequestMapping(value = "producer",method = {RequestMethod.POST})
public class ProducerController {

    @Autowired
    ProducerServerImpl producerServer;

    @ApiOperation(value = "新建生产者名",notes = "不需要指定Id")
    @RequestMapping("newProducer")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "isCycleSend",value = "0代表不循环发送，1代表循环发送",allowableValues = "0,1",dataType = "integer"),
//            @ApiImplicitParam(name = "isRandomAppend",value = "0代表不生成随机数，1代表生成随机数",allowableValues = "0,1",dataType = "integer"),
//            @ApiImplicitParam(name = "sendInterval",value = "发送间隔",dataType = "integer")
//    })
    public HttpResponse newProducer(
            @RequestBody ProducerInfo producerInfo
//                                       @RequestHeader int recordConnectionInfoId,
//                                       @RequestHeader String producerName,
//                                       @RequestHeader String producerTopic,
//                                       @RequestHeader(required = false) String comment,
//                                       @RequestHeader(required = false) String lastContent,
//                                       @RequestHeader(required = false,defaultValue = "TXT") String contentType,
//                                       @RequestHeader(required = false,defaultValue = "0") Integer isCycleSend,
//                                       @RequestHeader(required = false,defaultValue = "0") Integer isRandomAppend,
//                                       @RequestHeader(required = false,defaultValue = "100") Integer sendInterval
    ){
        try {
            producerServer.newProducer(producerInfo.getRecordConnectionInfoId(),producerInfo.getProducerName(),producerInfo.getProducerTopic(),
                    producerInfo.getComment(),producerInfo.getLastContent(),producerInfo.getContentType(),producerInfo.getIsCycleSend(),producerInfo.getIsRandomAppend(),
                    producerInfo.getSendInterval());
//            producerServer.newProducer(recordConnectionInfoId,producerName,producerTopic,comment,lastContent,contentType,isCycleSend,isRandomAppend,sendInterval);
        }catch (Exception e){
            return new HttpResponse<>(500,"添加失败,原因：" + e.toString(),null);
        }
        return new HttpResponse<>(200,"添加成功",null);
    }

    @ApiOperation("查询当前连接所有生产者")
    @RequestMapping("selectAllProducer")
    public HttpResponse<List<ProducerInfo>> selectAllProducer(@RequestHeader int recordConnectionInfoId){
        List<ProducerInfo> producerInfos;
        try {
            producerInfos = producerServer.selectAllProducer(recordConnectionInfoId);
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因：" + e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",producerInfos);
    }



    @ApiOperation("模糊查询生产者")
    @RequestMapping("selectProducerByNameAndConnectionId")
    public HttpResponse<List<ProducerInfo>> selectProducerByNameAndConnectionId(
                                                                                  @RequestBody FuzzySelect fuzzySelect
//                                                                                @RequestHeader String fuzzyString,
//                                                                                @RequestHeader int recordConnectionInfoId)
    ){
        List<ProducerInfo> producerInfos;
        try {
            producerInfos = producerServer.fuzzySelectProducerByNameAndConnectionId(fuzzySelect.fuzzyString,fuzzySelect.recordConnectionInfoId);
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",producerInfos);
    }


    @ApiOperation("删除指定生产者")
    @RequestMapping("deleteProducerById")
    public HttpResponse deleteProducerById(@RequestHeader int id){
        try {
            int i = producerServer.deleteProducer(id);
            if(i == 0)
                return new HttpResponse<>(500,"删除失败",null);
        }catch (Exception e){
            return new HttpResponse<>(500,"删除失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"删除成功",null);
    }

    @ApiOperation(value = "根据Id更新指定生产者",notes = "id,recordConnectionInfoId,isUsed,status不允许修改")
    @RequestMapping("updateProducerById")
    public HttpResponse updateConnectionById(@RequestBody ProducerInfo producerInfo){
        try {
            int i = producerServer.updateProducer(producerInfo);
            if(i == 0)
                return new HttpResponse<>(500,"更新失败",null);
        }catch (Exception e){
            return new HttpResponse<>(500,"更新失败,原因：" +e.toString(),null);
        }
        return new HttpResponse<>(200,"更新成功",null);
    }

    @ApiOperation("生产者发送数据")
    @RequestMapping("producerSend")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "sendAndStopFlag",value = "0代表暂停发送，1代表开始发送，2代表重发",allowableValues = "0,1,2",dataType = "integer")
    })
    public HttpResponse producerSend(@RequestHeader int producerId,
                                     @RequestHeader int sendAndStopFlag
                                     ){
        try {
            producerServer.producerSend(producerId,sendAndStopFlag);
        }catch (Exception e){
            return new HttpResponse<>(500,"状态变更失败,原因：" +e.toString(),null);
        }
        return new HttpResponse<>(200,"状态变更成功",null);
    }

    @ApiOperation("获取当前生产者最大id")
    @RequestMapping("getMaxProducerId")
    public HttpResponse<Integer> getMaxProducerId(){
        int maxProducerId;
        try {
            maxProducerId = producerServer.getMaxProducerId();
        }catch (Exception e){
            return new HttpResponse<>(500,"获取失败" +e.toString(),null);
        }
        return new HttpResponse<>(200,"获取成功",maxProducerId);
    }

}
