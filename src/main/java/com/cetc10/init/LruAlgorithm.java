package com.cetc10.init;

import com.cetc10.Mapper.ConsumerMapper;
import com.cetc10.Util.LRUCache;
import com.cetc10.domain.vo.WebSocketResponse;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "lru")
@Data
public class LruInit {
    @Autowired
    ConsumerMapper consumerMapper;
    private int capacity ;

    public HashMap<Integer, LRUCache<WebSocketResponse>> lruMap = new HashMap<>();

    @PostConstruct
    void init(){
        //查出所有的当前消费者id
        List<Integer> consumerIds = consumerMapper.selectAllConsumerId();
        for (int i : consumerIds) {
            lruMap.put(i, new LRUCache<>(capacity));
        }
    }
}
