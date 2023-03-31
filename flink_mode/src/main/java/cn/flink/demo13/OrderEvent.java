package cn.flink.demo13;


import lombok.Data;

@Data
public class OrderEvent{
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;


    public OrderEvent(Long orderId,String eventType,String txId,Long eventTime){
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.eventTime = eventTime;
    }



}