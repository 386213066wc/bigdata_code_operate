package cn.flink.demo7;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FlinkBroadCastByJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        //用户姓名以及年龄
        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add("zhangsan 30");
        arrayList.add("lisi 28");
        arrayList.add("wangwu 18");

        //将用户住址信息给广播出去
        ArrayList<String> arrayList2 = new ArrayList<>();
        arrayList2.add("zhangsan beijing");
        arrayList2.add("lisi shanghai");
        arrayList2.add("wangwu shenzhen");

        DataSource<ArrayList<String>> userAndAge = executionEnvironment.fromElements(arrayList);

        DataSource<String> stringDataSource = executionEnvironment.fromCollection(arrayList2);


        FlatMapOperator<ArrayList<String>, String> result = userAndAge.flatMap(new RichFlatMapFunction<ArrayList<String>, String>() {
            private HashMap<String, String> userAddress = new HashMap<String, String>();


            //在这里 获取广播变量的值
            @Override
            public void open(Configuration parameters) throws Exception {

                List<String> userAndAddress = getRuntimeContext().getBroadcastVariable("userAndAddress");
                for (String andAddress : userAndAddress) {
                    String[] s = andAddress.split(" ");
                    userAddress.put(s[0], s[1]);
                }

            }

            @Override
            public void flatMap(ArrayList<String> value, Collector<String> out) throws Exception {
                for (String s : value) {
                    String[] s1 = s.split(" ");
                    String age = userAddress.get(s1[0]);
                    out.collect(s1[0] + "\t" + s1[1] + "\t" + age);
                }
            }

        }).withBroadcastSet(stringDataSource, "userAndAddress");//进行数据的广播
        result.print();
    }
}