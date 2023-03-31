package cn.flink.demo12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OrderedStreamWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.getConfig().setAutoWatermarkInterval(5000);
        //默认就是使用EventTime来作为数据时间依据，可以不用添加也行
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Tuple2<String, Long> tuple2 = new Tuple2<>();

        DataStreamSource<String> socketTextStream = environment.socketTextStream("bigdata01", 9999);

        //将数据构建成为元祖
        SingleOutputStreamOperator<Tuple2<String, Long>> mapStream = socketTextStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] s1 = s.split(",");
                Tuple2<String, Long> stringLongTuple2 = new Tuple2<>();
                stringLongTuple2.setFields(s1[0], Long.valueOf(s1[1]));
                return stringLongTuple2;
            }
        });




        //forMonotonousTimestamps 仅仅表示周期性的添加一些假数据，作为水位线的触发条件
        SingleOutputStreamOperator<Tuple2<String, Long>> waterMarkStream = mapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            //覆写extractTimestamp 这个方法来抽取哪一个字段作为水位线的值
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                return element.f1;
            }
        }));


        waterMarkStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String,Long>, String, TimeWindow>() {


                    //对窗口当中的数据进行全量的处理
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                        long startTime = context.window().getStart();
                        long endTime = context.window().getEnd();
                        long watermark = context.currentWatermark();
                        long sum = 0L;
                        for (Tuple2<String, Long> element : elements) {
                             sum += 1 ;
                        }
                        Tuple2<String,Long> stringLongTuple2 = new Tuple2<>();
                        stringLongTuple2.setFields(s,sum);
                        out.collect(stringLongTuple2);


                    }
                }).print();

        waterMarkStream.print();
        environment.execute();



    }


}
