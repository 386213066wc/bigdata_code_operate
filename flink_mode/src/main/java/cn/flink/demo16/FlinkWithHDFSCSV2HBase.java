package cn.flink.demo16;

import org.apache.flink.table.api.*;

public class FlinkWithHDFSCSV2HBase {

    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.useBlinkPlanner()//Flink1.14开始就删除了其他的执行器了，只保留了BlinkPlanner，默认就是
                //.inStreamingMode()//默认就是StreamingMode
                //.inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        String source_sql = "CREATE TABLE source_hdfs (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  date_time STRING,\n" +
                "  email STRING" +
                ") WITH ( \n " +
                " 'connector' = 'filesystem',\n" +
                " 'path' = 'hdfs://bigdata01:8020/output_csv/userbase.csv/' , \n" +
                " 'format' = 'csv'\n" +
                ")";


        String sink_sql = "CREATE TABLE sink_table (\n" +
                " rowkey Integer,\n" +
                " f1 ROW<name STRING,email STRING,date_time STRING > ,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED \n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'hTable',\n" +
                " 'zookeeper.quorum' = 'bigdata01:2181,bigdata02:2181,bigdata03:2181'\n" +
                ") ";


        String execute_sql = "insert  into sink_table select id as rowkey,ROW(name,email,date_time)  from source_hdfs ";

        tableEnvironment.executeSql(source_sql);
        tableEnvironment.executeSql(sink_sql);
        tableEnvironment.executeSql(execute_sql);
    }
}