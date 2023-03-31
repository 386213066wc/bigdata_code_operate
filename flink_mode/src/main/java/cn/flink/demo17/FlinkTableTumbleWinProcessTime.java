package cn.flink.demo17;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 使用tableAPI方式来实现 传感器温度的求取
 * 使用tableAPI来基于窗口的操作
 */
public class FlinkTableTumbleWinProcessTime {

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
        //将流式数据转换成为一张表
        Table table = tEnv.fromDataStream(tempSensorData, $("sensorID"), $("tp"), $("temp"), $("ptTime").proctime());

        Table resultTable = table.window(Tumble.over(lit(5).second())
                        .on($("ptTime"))
                        .as("w")
                ).groupBy($("sensorID"), $("w"))
                .select($("sensorID"), $("temp").avg());


        resultTable.execute().print();




    }

}
