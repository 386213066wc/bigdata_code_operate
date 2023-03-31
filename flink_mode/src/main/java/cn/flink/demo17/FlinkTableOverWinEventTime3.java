package cn.flink.demo17;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class FlinkTableOverWinEventTime3 {
    public static void main(String[] args) {
        //1.获取stream的执行环境
        StreamExecutionEnvironment senv= StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        //2.创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(senv);

        //3.读取数据
        WatermarkStrategy<TempSensorData> watermarkStrategy = WatermarkStrategy
                .<TempSensorData>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TempSensorData>() {
                    @Override
                    public long extractTimestamp(TempSensorData t, long l) {
                        return t.getTp()*1000;
                    }
                });
        DataStream<TempSensorData> tempSensorData=senv.socketTextStream("bigdata01",8888)
                .map(event -> {
                    String[] arr = event.split(",");
                    return TempSensorData.builder()
                            .sensorID(arr[0])
                            .tp(Long.parseLong(arr[1]))
                            .temp(Integer.parseInt(arr[2]))
                            .build();
                }).assignTimestampsAndWatermarks(watermarkStrategy);

        //4.流转换为动态表
        Table table = tEnv.fromDataStream(tempSensorData,
                $("sensorID"),$("tp"),$("temp"),$("evTime").rowtime());

        //5.自定义窗口并计算
        Table resultTable = table.window(
                        Over.partitionBy($("sensorID"))
                                .orderBy($("evTime"))
                                .preceding(lit(5).seconds())
                                .as("w")
                )
                .select($("sensorID"),$("temp"),$("temp").max().over($("w")).as("maxTemp"));

        //6.执行Flink
        resultTable.execute().print();

    }
}