package com.cetc10.webSocket;


import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ServerEndpoint("/webSocketConsumer/{wid}")
public class WebsocketConsumerSever {
    public Logger log = LoggerFactory.getLogger(getClass());

    private String wid = "";

    private Session session;

    private static int onlineCount = 0;

    private static ConcurrentHashMap<String, WebsocketConsumerSever> websocketMap = new ConcurrentHashMap<>();

    @OnOpen
    public void onOpen(@PathParam(value = "wid") String wid, Session session) {
        this.session = session;
        this.wid = wid;
        if (!websocketMap.containsKey(wid)) {
            websocketMap.put(wid, this);
            addOnlineCount();
            log.info("打开websocket，wid为: " + wid + "，当前连接数" + getOnlineCount());
//            System.out.println("打开websocket，wid为: " + wid + "，当前连接数" + getOnlineCount());
            try {
                sendMessage("WebSocketOnOpen");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            log.info("websocket连接wid为: " + wid + "的链接已建立,当前连接数" + getOnlineCount());
//            System.out.println("websocket连接wid为: " + wid + "的链接已建立,当前连接数" + getOnlineCount());
        }

    }

    @OnClose
    public void onClose() {
        if (websocketMap.containsKey(this.wid)) {
            websocketMap.remove(this.wid);
            subOnlineCount();
            log.info("关闭websocket，wid为: " + wid + "，当前连接数" + getOnlineCount());
        }
    }

    @OnError
    public void OnError(Session session, Throwable error) {
        error.printStackTrace();
    }

    @OnMessage
    public void OnMessage(String message, Session session) {
        try {
//              session.getBasicRemote().sendText(message);
            sendMessage(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("来自客户端的消息" + message);

    }


    public static boolean sendToAll(Object message) {

        if (websocketMap!=null) {
            for (String key : websocketMap.keySet()) {
                try {
                    websocketMap.get(key).sendMessage(message);
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        } else {
            System.out.println("web端不在线");
        }
        return false;

    }


    public void sendMessage(Object message) throws IOException {
        if (message instanceof String) {

            this.session.getBasicRemote().sendText(String.valueOf(message));
        } else {

            this.session.getBasicRemote().sendText(JSON.toJSONString(message));
        }
    }
    public static void sendWebsocket(Object message, String sendClientId)  {
        if (websocketMap.get(sendClientId) != null) {
            try {
                websocketMap.get(sendClientId).sendMessage(message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("web端不在线");
        }
    }


    public static synchronized int getOnlineCount() {
        return onlineCount;
    }

    public static synchronized void addOnlineCount() {
        WebsocketConsumerSever.onlineCount++;
    }

    public static synchronized void subOnlineCount() {
        WebsocketConsumerSever.onlineCount--;
    }


    public String getWid() {
        return wid;
    }

    public Session getSession() {
        return session;
    }

    public static ConcurrentHashMap<String, WebsocketConsumerSever> getWebsocketMap() {
        return websocketMap;
    }
}
