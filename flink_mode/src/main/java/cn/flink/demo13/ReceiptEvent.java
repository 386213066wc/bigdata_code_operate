package cn.flink.demo13;

import lombok.Data;
@Data
public class ReceiptEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;

    public ReceiptEvent(String txId,String payChannel,Long eventTime){
        this.txId = txId;
        this.payChannel = payChannel;
        this.eventTime = eventTime;
    }
}