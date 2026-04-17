package org.example.takeaway_springboot.websocket;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
public class DataUpdateWebSocket {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    /**
     * 通知前端数据已更新
     */
    public void notifyDataUpdate() {
        messagingTemplate.convertAndSend("/topic/data-update", "DATA_UPDATED");
    }
}