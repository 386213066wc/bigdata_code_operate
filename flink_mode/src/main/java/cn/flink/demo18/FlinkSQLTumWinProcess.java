package cn.flink.demo18;


import cn.flink.demo17.TempSensorData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLTumWinProcess {
    public static void main(String[] args) {
        //1.获取stream的执行环境
        StreamExecutionEnvironment senv= StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        //2.创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(senv);
        //3.读取数据
        DataStream<TempSensorData> tempSensorData=senv.socketTextStream("bigdata01",8888)
                .map(event -> {
                    String[] arr = event.split(",");
                    return TempSensorData.builder()
                            .sensorID(arr[0])
                            .tp(Long.parseLong(arr[1]))
                            .temp(Integer.parseInt(arr[2]))
                            .build();
                });
        //4.流转换为动态表
        Table table = tEnv.fromDataStream(tempSensorData,
                $("sensorID"),$("tp"),$("temp"),$("ptTime").proctime());
        //5.自定义窗口并计算  支持直接使用sql语句的方式来查询数据
        Table resultTable = tEnv.sqlQuery("select "+
                "sensorID,"+
                "max(temp),"+
                "TUMBLE_START(ptTime,INTERVAL '5' second) as winstart "+
                "from "+table+" GROUP BY sensorID,TUMBLE(ptTime,INTERVAL '5' second) ");

        //6.执行Flink
        resultTable.execute().print();
    }
}