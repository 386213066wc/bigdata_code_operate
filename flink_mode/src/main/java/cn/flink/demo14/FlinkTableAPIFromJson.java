package cn.flink.demo14;


import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableAPIFromJson {

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
        tEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .format("json")
                .option("path","hdfs://bigdata01:8020/datas/click.log")
                .option("json.ignore-parse-errors","true")
                .build()
        );

        //3.创建sink table
        tEnv.createTemporaryTable("sinkTable",TableDescriptor.forConnector("print")
                .schema(schema)
                .build());

        //4.执行Table API 查询
        Table resultTable = tEnv.from("sourceTable").select($("user"),$("url"),$("cTime"));

        //5.输出
        resultTable.executeInsert("sinkTable");

    }
}