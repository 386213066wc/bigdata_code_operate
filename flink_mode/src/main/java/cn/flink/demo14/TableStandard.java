package cn.flink.demo14;


import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class TableStandard {

    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
             //   .useBlinkPlanner()//Flink1.14开始就删除了其他的执行器了，只保留了BlinkPlanner，默认就是
                //.inStreamingMode()//默认就是StreamingMode
                //.inBatchMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建source table: 1）读取外部表；2）从Table API或者SQL查询结果创建表
        Table projTable = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "zhangsan"),
                row(2L, "lisi")
        ).select($("id"), $("name"));

        //注册表到catalog(可选的)
        tEnv.createTemporaryView("sourceTable", projTable);

        //3、创建sink table  定义数据保存的表结构
        final Schema schema = Schema.newBuilder()
                .column("id", DataTypes.DECIMAL(10, 2))
                .column("name", DataTypes.STRING())
                .build();

        //https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/print/
        tEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("print")
                .schema(schema)
                .build());

        //4、Table API执行查询(可以执行多次查询，中间表可以注册到catalog也可以不注册)
        Table resultTable = tEnv.from("sourceTable").select($("id"), $("name"));
        //如果不注册sourceTable，可以这么写
        //Table resultTable = projTable.select($("id"), $("name"));

        //5、输出(包括执行,不需要单独在调用tEnv.execute("job"))
        resultTable.executeInsert("sinkTable");

    }


}