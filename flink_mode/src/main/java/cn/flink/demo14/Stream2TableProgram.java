package cn.flink.demo14;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Stream2TableProgram {
    public static void main(String[] args) throws Exception {

        //将StreamExecutionEnvironment转换成为StreamTableEnvironment

        //1、获取Stream执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、创建表执行环境
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inBatchMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,environmentSettings);

        env.setParallelism(1);
        env.setParallelism(1);
        //3、读取数据(source)
        DataStream<ClickLogs> clickLogs = env.fromElements(
                "Mary,./home,2022-02-02 12:00:00",
                "Bob,./cart,2022-02-02 12:00:00",
                "Mary,./prod?id=1,2022-02-02 12:00:05",
                "Liz,./home,2022-02-02 12:01:00",
                "Bob,./prod?id=3,2022-02-02 12:01:30",
                "Mary,./prod?id=7,2022-02-02 12:01:45"
        ).map(event -> {
            String[] props = event.split(",");
            ClickLogs clickLogs1 = new ClickLogs();
            clickLogs1.setUser(props[0]);
            clickLogs1.setUrl(props[1]);
            clickLogs1.setCTime(props[2]);
            return clickLogs1;
        });


        //4、流转换为动态表
        Table table = tEnv.fromDataStream(clickLogs);
        //5、执行Table API查询/SQL查询
        // select user,url,cTime from userTable where user  = 'mary';

        Table resultTable = table
                .where($("user").isEqual("Mary"))
                .select($("user"), $("url"), $("cTime"));

        //6、将Table转换为DataStream
        DataStream<ClickLogs> selectedClickLogs = tEnv.toDataStream(resultTable, ClickLogs.class);

        //7、处理结果：打印/输出
        selectedClickLogs.print();

        //8、执行
        env.execute("MixFlinkTableAndDataStream");





    }

}
