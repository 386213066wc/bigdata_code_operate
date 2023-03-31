package cn.flink.demo18;

import cn.flink.demo17.TempSensorData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLOverwinProcessRowRanger {
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
        Table resultTable = tEnv.sqlQuery("select "+
                "sensorID,"+
                "max(temp) OVER w AS max_temp,"+
                "avg(temp) OVER w AS avg_temp "+
                "from "+table+" WINDOW w AS (\n" +
                " PARTITION BY sensorID\n" +
                " ORDER BY ptTime\n" +
                " ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)\n");
        //6.执行Flink
        resultTable.execute().print();
    }
}