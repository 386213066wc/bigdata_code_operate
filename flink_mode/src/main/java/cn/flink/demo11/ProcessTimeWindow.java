package cn.flink.demo11;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class ProcessTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = environment.socketTextStream("bigdata01", 9999);
        Tuple2<String, Integer> tuple2 = new Tuple2<String,Integer>();
        socketTextStream.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                }).map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2 map(String value) throws Exception {
                        tuple2.setFields(value,1);
                        return tuple2;
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Double, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, Double, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Double> out) throws Exception {
                        int totalNum = 0;
                        int countNum = 0;
                        for (Tuple2<String, Integer> stringIntegerTuple2 : elements) {
                            totalNum +=1;
                            countNum += stringIntegerTuple2.f1;
                        }
                        out.collect(Double.valueOf(countNum/totalNum));
                    }
                }).print();

        environment.execute();
    }
}
