package cn.flink.demo12;


import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OutOfOrderStreamLateAndSideOutputByJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //默认就是使用EventTime来作为数据时间依据，可以不用添加也行
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        OutputTag<Tuple2<String, Long>> late = new OutputTag<Tuple2<String, Long>>("late");


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

        SingleOutputStreamOperator<Tuple2<String, Long>> result = mapStream.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
                    @Override
                    public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

                        WatermarkGenerator<Tuple2<String, Long>> waterMarkGenerator = new WatermarkGenerator<Tuple2<String, Long>>() {
                            private final Long maxOutofOrderness = 3000L;
                            private Long currentMaxTimestamp = 0L;

                            @Override
                            public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                //  currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                                Long f1 = event.f1;
                                Watermark watermark = new Watermark(f1);
                                output.emitWatermark(watermark);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                    /*    System.out.println("hello world" + "============" + currentMaxTimestamp);
                        Long water = currentMaxTimestamp - maxOutofOrderness -1 ;
                        System.out.println("时间水位线值为"+water);
                        Watermark watermark = new Watermark(water);
                        output.emitWatermark(watermark);*/

                            }
                        };
                        return waterMarkGenerator;
                    }
                }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))  //允许数据迟到多少秒钟
                .sideOutputLateData(late)  ///将迟到太多的数据进行统一收集
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (Tuple2<String, Long> element : elements) {
                            out.collect(element);
                        }
                    }
                });

        //获取迟到太多的数据
        /**
         * 一般像这种迟到太多的数据，可以发送到kafka另外一个topic里面去，或者写入到文件里面去
         * 每个窗口里面统计订单金额总和
         */
        result.getSideOutput(late).print();//进行补数的逻辑

        /**
         * 这里的window操作选型一共有以下四种，其中 SlidingEventTimeWindows   TumblingEventTimeWindows 都不能使用，
         * 否则就直接报错  Record has Long.MIN_VALUE timestamp (= no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?
         * SlidingEventTimeWindows
         * SlidingProcessingTimeWindows
         * TumblingProcessingTimeWindows
         * TumblingEventTimeWindows
         */
        environment.execute();

    }
}