package cn.flink.sql.opt;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class FlinkTableStructer {

    public static void main(String[] args) {

        //读取一些数据，注册成为一张表

        //获取核心编程对象TableEnvironment

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);


        //从集合当中获取数据
        Table projTable = tEnv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())

                ),

                row(1, "zhangsan"),
                row(2, "lisi")
        ).select($("id"), $("name"));

        //将table注册成为一张表
        tEnv.createTemporaryView("sourceTable",projTable);
        //对table进行查询
        tEnv.sqlQuery("select * from sourceTable").execute().print();



    }


}
