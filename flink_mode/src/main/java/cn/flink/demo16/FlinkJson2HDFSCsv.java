package cn.flink.demo16;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FlinkJson2HDFSCsv {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.useBlinkPlanner()//Flink1.14开始就删除了其他的执行器了，只保留了BlinkPlanner，默认就是
                //.inStreamingMode()//默认就是StreamingMode
                .inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        String source_sql = "CREATE TABLE json_table (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='input/userbase.json',\n" +
                "  'format'='json'\n" +
                ")";


        String sink_sql = "CREATE TABLE sink_hdfs (\n" +
                "  id Integer,\n" +
                "  name STRING,\n" +
                "  email STRING,\n" +
                "  date_time STRING" +
                ") WITH ( \n " +
                " 'connector' = 'filesystem',\n" +
                " 'path' = 'hdfs://bigdata01:8020/output_csv/userbase.csv' , \n" +
                " 'format' = 'csv'\n" +
                ")";


        String insert_SQL = "insert into sink_hdfs select id,name ,date_time,email from json_table ";


        //注册表
        tableEnvironment.executeSql(source_sql);
        tableEnvironment.executeSql(sink_sql);
        tableEnvironment.executeSql(insert_SQL);

    }
}