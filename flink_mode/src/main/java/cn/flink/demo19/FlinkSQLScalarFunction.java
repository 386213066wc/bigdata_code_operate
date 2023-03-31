package cn.flink.demo19;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FlinkSQLScalarFunction {

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
        //注册自定义函数
        tableEnvironment.createTemporarySystemFunction("JsonParse",JsonParseFunction.class);


        String source_sql = "CREATE TABLE json_table (\n" +
                "  line STRING \n" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='input/userbase.json',\n" +
                "  'format'='raw'\n" +
                ")";

        tableEnvironment.executeSql(source_sql);
        tableEnvironment.sqlQuery("select * from json_table").execute().print();

        tableEnvironment.sqlQuery("select JsonParse(line,'date_time'),JsonParse(line,'email'),JsonParse(line,'id'),JsonParse(line,'name') from json_table")
                .execute().print();

    }

}