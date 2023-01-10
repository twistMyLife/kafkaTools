package com.cetc10.server;

import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

@Service
public class CommonServerImpl {

    public void JsonVerify(String jsonSting){
        Object parse = JSONObject.parse(jsonSting);
        System.out.println(parse);
    }
}
