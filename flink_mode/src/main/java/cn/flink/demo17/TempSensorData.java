package cn.flink.demo17;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TempSensorData {
    //传感器ID
    private String sensorID;
    //时间戳
    private Long tp;
    //温度
    private Integer temp;
}
