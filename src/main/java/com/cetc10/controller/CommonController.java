package com.cetc10.controller;

import com.cetc10.domain.po.ConsumerInfo;
import com.cetc10.domain.vo.HttpResponse;
import com.cetc10.domain.vo.JsonBody;
import com.cetc10.server.CommonServerImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api("通用接口")
@RestController
@RequestMapping(value = "common",method = {RequestMethod.POST})
public class CommonController {

    @Autowired
    CommonServerImpl commonServer;


    @ApiOperation("JSON校验")
    @RequestMapping("jsonCheck")
    public HttpResponse<List<ConsumerInfo>> jsonCheck(@RequestBody JsonBody jsonBody) {
        try {
            commonServer.JsonVerify(jsonBody.getJsonString());
        }catch (Exception e){
            return new HttpResponse<>(500,"json校验失败,原因："+ e.toString(),null);
        }
        return new HttpResponse<>(200,"json校验成功",null);


    }



}
