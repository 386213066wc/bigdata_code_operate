package cn.flink.demo15;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableAPI2Kafka {

    public static void main(String[] args) {
        //1.创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2.创建 source table
        Schema schema = Schema.newBuilder()
                .column("user", DataTypes.STRING())
                .column("url",DataTypes.STRING())
                .column("cTime",DataTypes.STRING())
                .build();
        tEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("kafka")
                .schema(schema)
                .option("topic","clicklog_input")
                .option("properties.bootstrap.servers","bigdata01:9092")
                .option("properties.group.id","consumerkafka2kafka")
                .option("scan.startup.mode","earliest-offset")
                .option("format","json")
                .build()
        );

        //3.创建sink table
        tEnv.createTemporaryTable("sinkTable",TableDescriptor.forConnector("kafka")
                .schema(schema)
                .format("csv")
                .option("topic","clicklog_output")
                .option("properties.bootstrap.servers","bigdata01:9092")
                .build());

        //4.执行Table API 查询
        Table resultTable = tEnv.from("sourceTable").select($("user"),$("url"),$("cTime"));
        //5.输出
        resultTable.executeInsert("sinkTable");
    }


}
