package cn.flink.hudi;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ChangeLogJsonKafka {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
             //   .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        String sourceTable = "CREATE TABLE mysql_orders " +
                "(order_id INT," +
                "order_date timestamp(5) , " +
                "customer_name STRING, " +
                "price DECIMAL(10, 2), " +
                "product_id INT, " +
                "order_status BOOLEAN ) " +
                "WITH ('connector' = 'mysql-cdc'," +
                "'hostname' = 'bigdata03'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'testdb', " +
                "'table-name' = 'orders')" ;


        String kafkaSinkTable  = "CREATE TABLE kafka_gmv " +
                "(  day_str STRING, gmv DECIMAL(10, 5),primary key (day_str) not enforced )  " +
                "WITH ( 'connector' = 'upsert-kafka'," +
                "'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092', " +
                "'topic' = 'kafka_gmv', " +
                " 'key.format' = 'json' , 'value.format'='json' )";

        String aggregateSQL  = " INSERT INTO kafka_gmv " +
                "SELECT DATE_FORMAT(order_date, 'yyyy-MM-dd') as day_str, SUM(price) as gmv " +
                "FROM mysql_orders WHERE order_status = true " +
                "GROUP BY DATE_FORMAT(order_date, 'yyyy-MM-dd')";


        tableEnvironment.executeSql(sourceTable);
        tableEnvironment.executeSql(kafkaSinkTable);
        tableEnvironment.executeSql(aggregateSQL);

        Table resultTable = tableEnvironment.sqlQuery("select * from mysql_orders");

        DataStream<Tuple2<Boolean, MySqlOrders>> tuple2DataStream = tableEnvironment.toRetractStream(resultTable, MySqlOrders.class);
        tuple2DataStream.print();

        environment.execute();

    }
}