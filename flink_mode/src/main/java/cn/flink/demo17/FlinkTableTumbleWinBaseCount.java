package cn.flink.demo17;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkTableTumbleWinBaseCount {
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

        //5.自定义窗口并计算
        Table resultTable = table.window(
                        Tumble.over(rowInterval(5L))//基于数据的条数来进行往前滚动的
                                .on($("ptTime"))
                                .as("w")
                ).groupBy($("sensorID"),$("w"))
                .select($("sensorID"),$("temp").avg());

        //6.执行Flink
        resultTable.execute().print();
    }
}