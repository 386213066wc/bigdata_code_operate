package cn.flink.demo12;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OutOfOrderStreamWithPeriodicByJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //    environment.getConfig().setAutoWatermarkInterval(3000);
        //    environment.getConfig().setAutoWatermarkInterval(5000);
        //默认就是使用EventTime来作为数据时间依据，可以不用添加也行
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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


        mapStream.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
            //覆写了createWatermarkGenerator这个方法，这个方法，需要返回一个对象WatermarkGenerator
            @Override
            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

                WatermarkGenerator<Tuple2<String, Long>> watermarkGenerator = new WatermarkGenerator<Tuple2<String, Long>>() {

                    private final Long maxOutofOrderness = 3000L;
                    //定义每次最大的eventTime值为多少
                    private Long currentMaxTimestamp = 0L;

                    /**
                     * 每条数据都会进行调用，可以通过onevent 来进行获取最大的eventTime的值
                     * @param event
                     * @param eventTimestamp
                     * @param output
                     */
                    @Override
                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                    }
                    /**
                     * 为了往下游发送水位线的值
                     * @param output
                     */
                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        //获取water水位线的值
                        long water = currentMaxTimestamp - maxOutofOrderness - 1;
                        System.out.println("时间水位线的值为" + water);
                        Watermark watermark = new Watermark(water);
                        output.emitWatermark(watermark);

                    }
                };


                return watermarkGenerator;
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2)) // 表示允许数据的迟到2秒钟，也就是说数据去到了下一个窗口，但是触发的计算再等待两秒钟
          //      .sideOutputLateData() //对迟到太多的数据进行收集处理
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String,Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (Tuple2<String, Long> element : elements) {
                            out.collect(element);
                        }
                    }
                }).print();


        environment.execute();



    }


}
