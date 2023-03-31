package cn.flink.demo16;

import org.apache.flink.table.api.*;

public class FlinkTableWithKafka2MySQL {

    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.useBlinkPlanner()//Flink1.14开始就删除了其他的执行器了，只保留了BlinkPlanner，默认就是
                //.inStreamingMode()//默认就是StreamingMode
                //.inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);


        String source_sql = "CREATE TABLE KafkaTable (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'usr_opt',\n" +
                "  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',\n" +
                "  'properties.group.id' = 'user_opt_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "   'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'\n" +
                ")";


        String sink_sql = "CREATE TABLE mysql_sink (\n" +
                "  id Integer,\n" +
                "  username STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://localhost:3306/user_log?characterEncoding=utf-8&serverTimezone=GMT%2B8',\n" +
                "  'driver' = 'com.mysql.jdbc.Driver',\n" +
                "  'table-name' = 'clicklog',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456'\n" +
                ")";
        String execute_sql = "insert into mysql_sink select id,name,email,date_time from KafkaTable";
        tableEnvironment.executeSql(source_sql);
        tableEnvironment.executeSql(sink_sql);
        tableEnvironment.executeSql(execute_sql);
    }
}