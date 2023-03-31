package cn.flink.demo15;

import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableAPI2MySQL {

    public static void main(String[] args) {
        //1.创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2.创建 source table
        Schema schema = Schema.newBuilder()
                .column("user_id", DataTypes.INT())
                .column("user_name",DataTypes.STRING())
                .column("user_pwd",DataTypes.STRING())
                .column("user_addr",DataTypes.STRING())
                .build();
        tEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("jdbc")
                .schema(schema)
                .option("url","jdbc:mysql://localhost:3306/mydb")
                .option("driver","com.mysql.jdbc.Driver")
                .option("table-name","my_user")
                .option("username","root")
                .option("password","123456")
                .build()
        );

        //3.创建sink table
        Schema sinkschema = Schema.newBuilder()
                .column("user_name", DataTypes.STRING().notNull())
                .column("user_count",DataTypes.BIGINT())
                .primaryKey("user_name")
                .build();
        tEnv.createTemporaryTable("sinkTable",TableDescriptor.forConnector("jdbc")
                .schema(sinkschema)
                .option("url","jdbc:mysql://localhost:3306/mydb")
                .option("driver","com.mysql.jdbc.Driver")
                .option("table-name","my_user_sink")
                .option("username","root")
                .option("password","123456")
                .build());

        //4.执行Table API 查询
        Table resultTable = tEnv.from("sourceTable")
                .groupBy($("user_name"))
                .aggregate($("user_id").count().as("count"))
                .select($("user_name").as("user_name"),$("count"));

        //5.输出
        resultTable.executeInsert("sinkTable");
    }

}