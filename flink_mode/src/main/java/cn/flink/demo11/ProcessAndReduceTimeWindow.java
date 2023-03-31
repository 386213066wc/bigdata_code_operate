package cn.flink.demo11;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessAndReduceTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = environment.socketTextStream("bigdata01", 9999);
        Tuple2<String, Integer> tuple2 = new Tuple2<>();
        socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {

            @Override
            public Tuple2 map(String word) throws Exception {
                tuple2.setFields(word,1);
                return tuple2;
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(7)))//实现增量的处理
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        Tuple2<String, Integer> tuple21 = new Tuple2<>();
                        tuple21.setFields(value1.f0,value1.f1 + value2.f1);
                        return tuple21;
                    }
                },new MyCustomProcessWindowFunction()).print();


        environment.execute();

    }


}


class MyCustomProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,String,TimeWindow>{

    FastDateFormat instance = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    @Override
    public void process(String key, ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

        System.out.println("当前系统时间为"+instance.format(System.currentTimeMillis()));
        System.out.println("window的处理时间为"+instance.format(context.currentProcessingTime()));
        System.out.println("window的开始时间为"+instance.format(context.window().getStart()));
        System.out.println("window的结束时间为"+instance.format(context.window().getEnd()));

        Integer sum = 0;
        for (Tuple2<String, Integer> element : elements) {
            sum += element.f1;
        }
        Tuple2<String, Integer> tuple2 = new Tuple2<>();
        tuple2.setFields(key,sum);
        out.collect(tuple2);


    }
}

