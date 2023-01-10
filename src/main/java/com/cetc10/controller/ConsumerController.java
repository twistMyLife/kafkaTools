package com.cetc10.controller;


import com.cetc10.domain.vo.HttpResponse;
import com.cetc10.domain.vo.FuzzySelect;
import com.cetc10.domain.dto.OffsetLogsizeLag;
import com.cetc10.domain.po.ConsumerInfo;
import com.cetc10.server.ConsumerServerImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api("消费者相关接口")
@RestController
@RequestMapping(value = "consumer",method = {RequestMethod.POST})
public class ConsumerController {

    @Autowired
    ConsumerServerImpl consumerServer;

    @ApiOperation(value = "新建消费者",notes = "不需要指定Id")
    @RequestMapping("newConsumer")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "isCycleSend",value = "0代表不循环发送，1代表循环发送",allowableValues = "0,1",dataType = "integer"),
//            @ApiImplicitParam(name = "isRandomAppend",value = "0代表不生成随机数，1代表生成随机数",allowableValues = "0,1",dataType = "integer"),
//            @ApiImplicitParam(name = "sendInterval",value = "发送间隔",dataType = "integer")
//    })
    public HttpResponse newConsumer(
            @RequestBody ConsumerInfo consumerInfo
    ){
        try {
            consumerServer.newConsumer(consumerInfo.getRecordConnectionInfoId(),consumerInfo.getConsumerName(),consumerInfo.getConsumerTopic(),
                    consumerInfo.getConsumerGroupName(),consumerInfo.getComment(),consumerInfo.getAutoCommit(),consumerInfo.getStatus());

        }catch (Exception e){
            return new HttpResponse<>(500,"添加失败,原因：" + e.toString(),null);
        }
        return new HttpResponse<>(200,"添加成功",null);
    }

    @ApiOperation("查询当前连接所有消费者")
    @RequestMapping("selectAllConsumer")
    public HttpResponse<List<ConsumerInfo>> selectAllConsumer(@RequestHeader int recordConnectionInfoId){
        List<ConsumerInfo> consumerInfos;
        try {
            consumerInfos = consumerServer.selectAllConsumer(recordConnectionInfoId);
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因：" + e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",consumerInfos);
    }



    @ApiOperation("模糊查询消费者")
    @RequestMapping("selectConsumerByNameAndConnectionId")
    public HttpResponse<List<ConsumerInfo>> selectConsumerByNameAndConnectionId(
      @RequestBody FuzzySelect fuzzySelect){
        List<ConsumerInfo> consumerInfos;
        try {
            consumerInfos = consumerServer.fuzzySelectConsumerByNameAndConnectionId(fuzzySelect.fuzzyString,fuzzySelect.recordConnectionInfoId);
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",consumerInfos);
    }


    @ApiOperation("删除指定消费者")
    @RequestMapping("deleteConsumerById")
    public HttpResponse deleteConsumerById(@RequestHeader int id){
        try {
            int i = consumerServer.deleteConsumer(id);
            if(i == 0)
                return new HttpResponse<>(500,"删除失败",null);
        }catch (Exception e){
            return new HttpResponse<>(500,"删除失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"删除成功",null);
    }

    @ApiOperation(value = "根据Id更新指定消费者",notes = "id,recordConnectionInfoId,status不允许修改")
    @RequestMapping("updateConsumerById")
    public HttpResponse updateConsumerById(@RequestBody ConsumerInfo consumerInfo){
        try {
            int i = consumerServer.updateConsumer(consumerInfo);
            if(i == 0)
                return new HttpResponse<>(500,"更新失败",null);
        }catch (Exception e){
            return new HttpResponse<>(500,"更新失败,原因：" +e.toString(),null);
        }
        return new HttpResponse<>(200,"更新成功",null);
    }

    @ApiOperation("消费者消费数据")
    @RequestMapping("consumerConsume")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "receiveAndStopFlag",value = "0代表退出消费者组(内存中销毁此消费者)，1代表开始消费",allowableValues = "0,1",dataType = "integer")
    })
    public HttpResponse consumerConsume(@RequestHeader int consumerId,
                                     @RequestHeader int receiveAndStopFlag
                                     ){
        try {
            consumerServer.consumerConsume(consumerId,receiveAndStopFlag);
        }catch (Exception e){
            return new HttpResponse<>(500,"状态变更失败,原因：" +e.toString(),null);
        }
        return new HttpResponse<>(200,"状态变更成功",null);
    }

    @ApiOperation("消费者设置offset")
    @RequestMapping("setConsumerOffset")
    public HttpResponse setConsumerOffset(@RequestHeader int consumerId,
                                          @RequestHeader long offset
    ){
        try {
            consumerServer.setConsumerOffset(consumerId,offset);
        }catch (Exception e){
            e.printStackTrace();
            return new HttpResponse<>(500,"设置失败,原因：" +e.toString(),null);
        }
        return new HttpResponse<>(200,"设置成功",null);
    }

    @ApiOperation("查询当前消费者组的offsetLogSizeLag")
    @RequestMapping("selectOffsetLogSizeLag")
    public HttpResponse<OffsetLogsizeLag> selectOffsetLogSizeLag(@RequestHeader int consumerId
    ){
        OffsetLogsizeLag offsetLogsizeLag = null;
        try {
            offsetLogsizeLag = consumerServer.selectOffsetLogSizeLag(consumerId);
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因：" +e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",offsetLogsizeLag);
    }

    @ApiOperation("获取当前消费者最大id")
    @RequestMapping("getMaxConsumerId")
    public HttpResponse<Integer> getMaxConsumerId(){
        int maxConsumerId;
        try {
            maxConsumerId = consumerServer.getMaxConsumerId();
        }catch (Exception e){
            return new HttpResponse<>(500,"获取失败" +e.toString(),null);
        }
        return new HttpResponse<>(200,"获取成功",maxConsumerId);
    }
}
