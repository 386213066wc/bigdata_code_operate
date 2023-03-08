package cn.flink.demo1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestParallelism {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //获取批量处理执行程序
        StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //执行环境级别的并行度
      //  localEnvironmentWithWebUI.setParallelism(4);
        DataStreamSource<String> dataStreamSource = localEnvironmentWithWebUI.socketTextStream("bigdata01", 9999);
        dataStreamSource.flatMap(new FlatMapFunction<String, String >() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).setParallelism(2).map(new MapFunction<String, Tuple2<String,Integer>>() {

            Tuple2<String, Integer> tuple2 = new Tuple2<>();
            @Override
            public Tuple2 map(String value) throws Exception {
                tuple2.setFields(value,1);
                return tuple2;
            }
        }).setParallelism(4).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1).setParallelism(8).print();

        localEnvironmentWithWebUI.execute();


    }


}