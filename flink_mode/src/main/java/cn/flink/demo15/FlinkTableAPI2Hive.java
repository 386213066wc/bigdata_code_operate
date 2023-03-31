package cn.flink.demo15;


import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class FlinkTableAPI2Hive {
    public static void main(String[] args) {

        //1.创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2.创建 source table
        Table sourceTable = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id",DataTypes.INT()),
                        DataTypes.FIELD("name",DataTypes.STRING()),
                        DataTypes.FIELD("age",DataTypes.INT())
                ),
                row(1,"zhangsan","18"),
                row(2,"lisi","28"),
                row(3,"wangwu","35"),
                row(4,"zhaoliu","20")
        ).select($("id"),$("name"),$("age"));

        //注册表
        tEnv.createTemporaryView("sourceTable",sourceTable);

        //3.查询操作
        Table resultTable = tEnv.from("sourceTable")
                .select($("id"),$("name"),$("age"));
        //打印执行计划
        System.out.println( resultTable.explain());
        //4.创建HiveCatalog
        String name = "myCataLog";
        String defaultDatabase = "default";
        String hiveConfDir = "input/hiveconf/";
        tEnv.loadModule(name,new HiveModule("3.1.2"));
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        HiveCatalog hive = new HiveCatalog(name,defaultDatabase,hiveConfDir);
        //注册catalog
        tEnv.registerCatalog(name,hive);
        //设置当前会话使用的catalog和database
        tEnv.useCatalog(name);
        tEnv.useDatabase(defaultDatabase);
        //5.查询结果插入hive
        tEnv.executeSql("insert overwrite table  users_output select name,cast (count(1)  as int )as user_count from  "+resultTable.toString() +"  group by name ");
    }
}