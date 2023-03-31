package cn.flink.demo14;


import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableAPIFromMySQL {
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
                .option("url","jdbc:mysql://bigdata03:3306/mydb")
                .option("driver","com.mysql.jdbc.Driver")
                .option("table-name","my_user")
                .option("username","root")
                .option("password","123456")
                .build()
        );
        //3.创建sink table
        tEnv.createTemporaryTable("sinkTable",TableDescriptor.forConnector("print")
                .schema(schema)
                .build());

        //4.执行Table API 查询
        Table resultTable = tEnv.from("sourceTable").select($("user_id"),$("user_name"),$("user_pwd"),$("user_addr"));
        //5.输出
        resultTable.executeInsert("sinkTable");
    }
}