package com.cetc10.controller;

import com.cetc10.domain.vo.HttpResponse;
import com.cetc10.domain.vo.FuzzySelect;
import com.cetc10.domain.po.ConnectionInfo;
import com.cetc10.server.ConnectionServerImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api("kafka连接信息")
@RestController
@RequestMapping(value = "connection",method = {RequestMethod.POST})
public class ConnectionController {

    @Autowired
    ConnectionServerImpl connectionServer;

    @ApiOperation("测试连接")
    @RequestMapping("testConnection")
    public HttpResponse testConnection(
                                      //@RequestHeader String kafkaName,
                                       @RequestHeader(required = false) String kafkaVersion,
                                       @RequestHeader String kafkaBootStrap,
                                       @RequestHeader(required = false) String zookeeperBootStrap
                               ){
        try{
            String con = connectionServer.testConnection( kafkaVersion, kafkaBootStrap);
                return new HttpResponse<>(200,con,null);
        }catch (Exception e){
            return new HttpResponse<>(500,"连接失败,原因：" + e.toString(),null);
        }
    }

    @ApiOperation("添加连接")
    @RequestMapping("addConnection")
    public HttpResponse addConnection(
            @RequestBody ConnectionInfo connectionInfo
//                               @RequestHeader String kafkaName,
//                               @RequestHeader(required = false) String kafkaVersion,
//                               @RequestHeader String kafkaBootStrap,
//                               @RequestHeader(required = false) String zookeeperBootStrap
    ){
        try {
            connectionServer.addConnection(connectionInfo.getKafkaName(),connectionInfo.getKafkaVersion(),
                    connectionInfo.getKafkaBootstrap(), connectionInfo.getZookeeperBootstrap());
        }catch (Exception e){
            return new HttpResponse<>(500,"添加失败,原因：" + e.toString(),null);
        }
        return new HttpResponse<>(200,"添加成功",null);
    }


    @ApiOperation("查询所有连接")
    @RequestMapping("selectAllConnection")
    public HttpResponse<List<ConnectionInfo>> selectAllConnection(){
        List<ConnectionInfo> connectionInfoList;
        try {
            connectionInfoList = connectionServer.selectAllConnection();
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因：" + e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",connectionInfoList);
    }

    @ApiOperation("根据kafkaName模糊查询连接")
    @RequestMapping("selectConnectionByKafkaName")
    public HttpResponse<List<ConnectionInfo>> selectConnectionByKafkaName(@RequestBody FuzzySelect fuzzySelect){
        List<ConnectionInfo> connectionInfos;
        try {
            connectionInfos = connectionServer.fuzzySelectConnectionByKafkaName(fuzzySelect.fuzzyString);
        }catch (Exception e){
            return new HttpResponse<>(500,"查询失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"查询成功",connectionInfos);
    }


    @ApiOperation("删除指定连接")
    @RequestMapping("deleteConnectionById")
    public HttpResponse deleteConnectionById(@RequestHeader int id){
        try {
            int i = connectionServer.deleteConnection(id);
            if(i == 0)
                return new HttpResponse<>(500,"删除失败",null);
        }catch (Exception e){
            return new HttpResponse<>(500,"删除失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"删除成功",null);
    }

    @ApiOperation("根据Id更新指定连接")
    @RequestMapping("updateConnectionById")
    public HttpResponse updateConnectionById(@RequestBody ConnectionInfo connectionInfo){
        try {
            int i = connectionServer.updateConnection(connectionInfo);
            if(i == 0)
                return new HttpResponse<>(500,"更新失败",null);
        }catch (Exception e){
            return new HttpResponse<>(500,"更新失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"更新成功",null);
    }

}
