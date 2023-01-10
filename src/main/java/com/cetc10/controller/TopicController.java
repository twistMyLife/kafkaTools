package com.cetc10.controller;

import com.cetc10.domain.vo.HttpResponse;
import com.cetc10.domain.dto.MessageResponseDto;
import com.cetc10.domain.po.TopicInfo;
import com.cetc10.server.TopicServerImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api("Topic相关接口")
@RestController
@RequestMapping(value = "topic",method = {RequestMethod.POST})
public class TopicController {
    @Autowired
    TopicServerImpl topicServer;

    @ApiOperation("新增topic")
    @RequestMapping("addTopic")
    public HttpResponse addTopic(@RequestHeader int recordConnectionInfoId,
                                @RequestHeader String topicName,
                                @RequestHeader(defaultValue = "1") Integer partitions,
                                @RequestHeader(defaultValue = "1") Integer replicas){
        try {
            topicServer.newTopic(recordConnectionInfoId,topicName,partitions,replicas);
        }catch (Exception e){
            return new HttpResponse<>(500,"添加失败,原因：" + e.toString(),null);
        }
        return new HttpResponse<>(200,"添加成功",null);

    }

    @ApiOperation("查询当前连接所有Topic列表")
    @RequestMapping("selectAllTopic")
    public HttpResponse<List<TopicInfo>> selectAllTopic(@RequestHeader int recordConnectionInfoId){
        List<TopicInfo> topicInfos;
        try {
            topicInfos = topicServer.selectAllTopic(recordConnectionInfoId);
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因：" + e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",topicInfos);
    }

    @ApiOperation("模糊查询Topic列表")
    @RequestMapping("selectTopicByNameAndConnectionId")
    public HttpResponse<List<TopicInfo>> selectTopicByNameAndConnectionId(@RequestHeader String fuzzyString,
                                                                                @RequestHeader int recordConnectionInfoId){
        List<TopicInfo> topicInfos;
        try {
            topicInfos = topicServer.fuzzySelectTopicByNameAndConnectionId(fuzzyString,recordConnectionInfoId);
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",topicInfos);
    }


    @ApiOperation("删除指定Topic列表")
    @RequestMapping("deleteTopicById")
    public HttpResponse deleteTopicById(@RequestHeader int id){
        try {
            int i = topicServer.deleteTopic(id);
            if(i == 0)
                return new HttpResponse<>(500,"删除失败",null);
        }catch (Exception e){
            return new HttpResponse<>(500,"删除失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"删除成功",null);
    }

    @ApiOperation("查询Topic中最近的n条数据")
    @RequestMapping("selectLatestTopicData")
    public HttpResponse<List<MessageResponseDto>> selectLatestTopicData(@RequestHeader int id,
                                              @RequestHeader int num){
        List<MessageResponseDto> resultList;
        try {
            resultList = topicServer.searchTopicDataById(id, num);
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",resultList);
    }

}
