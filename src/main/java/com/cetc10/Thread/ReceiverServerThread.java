package com.cetc10.Thread;

import com.cetc10.domain.po.ConsumerInfo;
import com.cetc10.server.ReceiverServerImpl;

public class ReceiverServerThread extends Thread {

    public int flag;
    private ReceiverServerImpl receiverServer;
    private ConsumerInfo consumerInfo;

    public ReceiverServerThread(int flag,ReceiverServerImpl receiverServer,ConsumerInfo consumerInfo){
        this.flag = flag;
        this.receiverServer = receiverServer;
        this.consumerInfo = consumerInfo;
    }

    @Override
    public void run() {
        while(true){
            if(flag == 0){
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else if(flag == 1){
                receiverServer.consume(consumerInfo.getConsumerTopic(),consumerInfo.getConsumerGroupName(),
                        consumerInfo.getAutoCommit() == 1);
            }
        }
    }


}
