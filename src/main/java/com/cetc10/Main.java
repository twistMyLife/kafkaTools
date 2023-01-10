package com.cetc10;

import com.cetc10.Util.KafkaUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import sun.rmi.runtime.Log;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class Main {
    public static void main(String[] args) {
        //400条数据
        List<Integer> list  = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        new Thread(()->{
            for (int i = 12001; i < 22001; i++) {
                list.add(i);
            }
            System.out.println(list);
        }).start();
        for (int i = 1001; i < 12001; i++) {
            list.add(i);
        }
    }

}
