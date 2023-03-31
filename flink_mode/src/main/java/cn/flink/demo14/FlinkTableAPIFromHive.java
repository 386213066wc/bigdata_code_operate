package cn.flink.demo14;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

public class FlinkTableAPIFromHive {
    public static void main(String[] args) {
        //1.创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        //2.创建HiveCatalog
        String name = "myCataLog";
        String defaultDatabase = "default";
        String hiveConfDir = "input/hiveconf/";
        tEnv.loadModule(name,new HiveModule("3.1.2"));
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        //获取hive当中的catalog，catalog就是元数据管理模块
        HiveCatalog hive = new HiveCatalog(name,defaultDatabase,hiveConfDir);
        //3.注册catalog
        tEnv.registerCatalog(name,hive);
        //4.设置当前会话使用的catalog和database
        tEnv.useCatalog(name);
        tEnv.useDatabase(defaultDatabase);
        //5.查询hive中的表数据
        tEnv.executeSql("select * from users").print();
    }


}
