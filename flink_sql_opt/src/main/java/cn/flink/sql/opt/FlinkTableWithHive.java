package cn.flink.sql.opt;

import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

public class FlinkTableWithHive {
    public static void main(String[] args) {
        //1.创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2.创建HiveCatalog
        String name = "myCataLog";
        String defaultDatabase = "test";
        String hiveConfDir = "input/hiveconf/";
        tEnv.loadModule(name,new HiveModule("3.1.2"));
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        HiveCatalog hive = new HiveCatalog(name,defaultDatabase,hiveConfDir);

        //3.注册catalog
        tEnv.registerCatalog(name,hive);

        //4.设置当前会话使用的catalog和database
        tEnv.useCatalog(name);
        tEnv.useDatabase(defaultDatabase);


        tEnv.executeSql("insert into user_count select username,count(1) as count_result from clicklog group by username");

    }
}