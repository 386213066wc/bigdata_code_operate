package cn.flink.demo17;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkSQLEvenTime {
    public static void main(String[] args) {
        //1.创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2.创建source table,这种方式会自动注册表
        tEnv.executeSql("CREATE TABLE sourceTable ("+
                " username STRING,"+
                " url STRING,"+
                " cTime TIMESTAMP(3),"+  //定义时间戳时间
                " WATERMARK FOR cTime AS cTime - INTERVAL '10' SECOND"+  //通过eventTime来定义watermark的值，最大允许的数据的乱序的时间为10s
                ") WITH ("+
                " 'connector' = 'filesystem',"+
                " 'path' = 'datas/click.log',"+
                " 'format' = 'json'"+
                ")");
        //3.Flink SQL 查询
        Table resultTable = tEnv.sqlQuery("select * from sourceTable");

        resultTable.printSchema();

        //4.执行Flink SQL
        resultTable.execute().print();
    }

}
