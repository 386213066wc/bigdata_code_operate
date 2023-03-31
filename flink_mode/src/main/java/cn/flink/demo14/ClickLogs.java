package cn.flink.demo14;

import lombok.Data;

@Data
public class ClickLogs {
    private String user;
    private String  url;
    private String cTime;
}
