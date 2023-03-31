package cn.flink.hudi;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLMaxwell2Kafka {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT,"8581-8587");
        StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(localEnvironmentWithWebUI, environmentSettings);

        String createTable = "CREATE TABLE t_binlog (\n" +
                "  id INT,\n" +
                "  NAME STRING,\n" +
                "  age INT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'kafka_cdc',\n" +
                " 'properties.bootstrap.servers' = 'bigdata01:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'maxwell-json',\n" +
                " 'scan.startup.mode' = 'latest-offset'\n" +
                ")";

        String queryTable = "select * from t_binlog";

        streamTableEnvironment.executeSql(createTable);

        Table result = streamTableEnvironment.sqlQuery(queryTable);

        streamTableEnvironment.toRetractStream(result, UserBean.class).print();

        localEnvironmentWithWebUI.execute();


    }
}