package cn.flink.demo13;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<String> orderLog = executionEnvironment.readTextFile("file:///E:\\1、课程内容\\1、课程资料\\11、Flink实时处理\\2、数据资料\\OrderLog.csv");

        SingleOutputStreamOperator<OrderEvent> orderEventStream = orderLog.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                OrderEvent orderEvent = new OrderEvent(Long.valueOf(split[0].trim()), split[1].trim(), split[2].trim(), Long.valueOf(split[3].trim()));

                return orderEvent;
            }
        }).filter(new FilterFunction<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return value.getTxId() != null && value.getTxId() != "";
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());




        DataStreamSource<String> dataStreamSource = executionEnvironment.readTextFile("file:///E:\\1、课程内容\\1、课程资料\\11、Flink实时处理\\2、数据资料\\ReceiptLog.csv");

        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = dataStreamSource.map(new MapFunction<String, ReceiptEvent>() {
            @Override
            public ReceiptEvent map(String value) throws Exception {
                String[] split = value.split(",");
                ReceiptEvent receiptEvent = new ReceiptEvent(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                return receiptEvent;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());


        DataStream<Tuple2<OrderEvent, ReceiptEvent>> result = orderEventStream.join(receiptEventStream).where(new KeySelector<OrderEvent, String>() {
                    @Override
                    public String getKey(OrderEvent value) throws Exception {
                        return value.getTxId();
                    }
                }).equalTo(new KeySelector<ReceiptEvent, String>() {
                    @Override
                    public String getKey(ReceiptEvent value) throws Exception {
                        return value.getTxId();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new MyJoinFunction());

        result.print("match");
        executionEnvironment.execute();
    }



}

class MyJoinFunction implements JoinFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>>{


    @Override
    public Tuple2<OrderEvent, ReceiptEvent> join(OrderEvent first, ReceiptEvent second) throws Exception {

        Tuple2<OrderEvent, ReceiptEvent> orderEventReceiptEventTuple2 = new Tuple2<>();
        orderEventReceiptEventTuple2.f0 = first;
        orderEventReceiptEventTuple2.f1= second;

        return orderEventReceiptEventTuple2;
    }
}