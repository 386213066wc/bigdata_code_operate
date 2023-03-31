package cn.flink.hudi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySqlCdc {
    public static void main(String[] args) throws Exception {
        //构建本地运行环境
        Configuration configuration = new Configuration();

        StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode()
                .build();
        //构建StreamTable对象
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(localEnvironmentWithWebUI, environmentSettings);

        //定义表结构，直接从mysql当中的binlog获取数据
        String createTable = "CREATE TABLE mysql_binlog " +
                "( id INT ,   NAME STRING,  age INT )" +
                " WITH (  'connector' = 'mysql-cdc'" +
                ", 'hostname' = 'bigdata03'" +
                ", 'port' = '3306'" +
                ", 'username' = 'root'" +
                ", 'password' = '123456'" +
                ", 'database-name' = 'testdb'" +
                ", 'table-name' = 'testUser'  )";

        //定义查询sql一句
        String queryTable = "select * from mysql_binlog";

        tableEnvironment.executeSql(createTable);
        Table result = tableEnvironment.sqlQuery(queryTable);
        //查询到的数据进行提取出来，转换成为DataStream进行打印
        DataStream<Tuple2<Boolean, UserBean>> tuple2DataStream = tableEnvironment.toRetractStream(result, UserBean.class);
        tuple2DataStream.print();
        localEnvironmentWithWebUI.execute();
    }
}