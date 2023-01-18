package cn.flink.sql.opt;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkTableWithHBase2Kafka {

    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.useBlinkPlanner()//Flink1.14开始就删除了其他的执行器了，只保留了BlinkPlanner，默认就是
                //.inStreamingMode()//默认就是StreamingMode
                //.inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        String source_table = "CREATE TABLE hTable (\n" +
                " rowkey STRING,\n" +
                " f1 ROW<username STRING,email STRING,date_time String> ,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED \n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'opt_log',\n" +
                " 'zookeeper.quorum' = 'bigdata01:2181,bigdata02:2181,bigdata03:2181'\n" +
                ") ";


        String sink_table = "CREATE TABLE KafkaTable (\n" +
                "  `username` STRING,\n" +
                "  `email` STRING,\n" +
                "  `date_time` STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_output',\n" +
                "  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',\n" +
                "  'format' = 'json'\n" +
                ")";


        String execute_sql = "insert into KafkaTable select username,email,date_time from hTable";

        tableEnvironment.executeSql(source_table);
        tableEnvironment.executeSql(sink_table);
        tableEnvironment.executeSql(execute_sql);





    }

}
