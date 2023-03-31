package cn.flink.demo17;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkSQLProcessTime {
    public static void main(String[] args) {
        //1.创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2.创建source table,这种方式会自动注册表
        tEnv.executeSql("CREATE TABLE sourceTable ("+
                " username STRING,"+
                " url STRING,"+
                " cTime STRING,"+
                " proctime as PROCTIME()"+  //使用proctime来作为process_time  以flink程序解析的时间为准
                ") WITH ("+
                " 'connector' = 'filesystem',"+
                " 'path' = 'datas/click.log',"+
                " 'format' = 'json'"+
                ")");
        //3.Flink SQL 查询
        Table resultTable = tEnv.sqlQuery("select * from sourceTable");

        resultTable.printSchema();

        //4.执行Flink SQL
        resultTable.execute().print();
    }
}
