package cn.flink.demo13;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoinByJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<String> orderLog = executionEnvironment.readTextFile("file:///E:\\1、课程内容\\1、课程资料\\11、Flink实时处理\\2、数据资料\\OrderLog.csv");

        KeyedStream<OrderEvent, String> orderEventStream = orderLog.map(new RichMapFunction<String, OrderEvent>() {
            private OrderEvent orderEvent;
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                orderEvent = new OrderEvent(Long.valueOf(split[0].trim()), split[1].trim(), split[2].trim(), Long.valueOf(split[3]));
                return orderEvent;
            }
        }).filter(new FilterFunction<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return value.getTxId() != "";
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1))).keyBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent value) throws Exception {

                return value.getTxId();
            }
        });




        DataStreamSource<String> dataStreamSource = executionEnvironment.readTextFile("file:///E:\\1、课程内容\\1、课程资料\\11、Flink实时处理\\2、数据资料\\ReceiptLog.csv");

        KeyedStream<ReceiptEvent, String> receipEventStream = dataStreamSource.map(new MapFunction<String, ReceiptEvent>() {
                    @Override
                    public ReceiptEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        ReceiptEvent receiptEvent = new ReceiptEvent(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                        return receiptEvent;
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(new KeySelector<ReceiptEvent, String>() {
                    @Override
                    public String getKey(ReceiptEvent value) throws Exception {
                        return value.getTxId();
                    }
                });


        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> result = orderEventStream.intervalJoin(receipEventStream)
                .between(Time.seconds(-3), Time.seconds(3))
                .lowerBoundExclusive()//排除下界的时间
                .upperBoundExclusive()//排除上界的时间
                .process(new MyProcessJoinFunction());
        result.print();
        executionEnvironment.execute();
    }
}

class MyProcessJoinFunction extends ProcessJoinFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>>{
    @Override
    public void processElement(OrderEvent left, ReceiptEvent right, ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        Tuple2<OrderEvent, ReceiptEvent> orderEventReceiptEventTuple2 = new Tuple2<>(left, right);
        out.collect(orderEventReceiptEventTuple2);
    }
}