package cn.flink.demo13;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ConnectJoin {
    public static void main(String[] args) throws Exception {
        //定义侧输出流
        OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatchedReceipts"){};
        OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatchedPays"){};

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> orderLog = executionEnvironment.readTextFile("file:///E:\\1、课程内容\\1、课程资料\\11、Flink实时处理\\2、数据资料\\OrderLog.csv");

        KeyedStream<OrderEvent, String> orderLogStream = orderLog.map(new MapFunction<String, OrderEvent>() {
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
                }).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(new KeySelector<OrderEvent, String>() {
                    @Override
                    public String getKey(OrderEvent value) throws Exception {
                        return value.getTxId();
                    }
                });


        DataStreamSource<String> dataStreamSource = executionEnvironment.readTextFile("file:///E:\\1、课程内容\\1、课程资料\\11、Flink实时处理\\2、数据资料\\ReceiptLog.csv");

        KeyedStream<ReceiptEvent, String> receiptStream = dataStreamSource.map(new MapFunction<String, ReceiptEvent>() {
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


        //得到join之后的流数据
        ConnectedStreams<OrderEvent, ReceiptEvent> connectedStream = orderLogStream.connect(receiptStream);

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> processStream = connectedStream.process(new CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>() {

            private ValueState<OrderEvent> orderEventValueState = null;
            private ValueState<ReceiptEvent> receiptEventValueState = null;


            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<OrderEvent> orderEventValueStateDescriptor = new ValueStateDescriptor<>("pay-state", OrderEvent.class);
                ValueStateDescriptor<ReceiptEvent> receiptEventValueStateDescriptor = new ValueStateDescriptor<ReceiptEvent>("pay-state", ReceiptEvent.class);

                orderEventValueState = getRuntimeContext().getState(orderEventValueStateDescriptor);
                receiptEventValueState = getRuntimeContext().getState(receiptEventValueStateDescriptor);
            }

            //专门进行orderEvent的解析
            @Override
            public void processElement1(OrderEvent orderEvent, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
                ReceiptEvent receiptEvent = receiptEventValueState.value();
                //对账流将数据已经到了
                if (receiptEvent != null) {

                    Tuple2<OrderEvent, ReceiptEvent> orderEventReceiptEventTuple2 = new Tuple2<>();
                    orderEventReceiptEventTuple2.setFields(orderEvent, receiptEvent);

                    receiptEventValueState.clear();

                } else {
                    //对账流数据还没到
                    orderEventValueState.update(orderEvent);
                    ctx.timerService().registerEventTimeTimer(orderEvent.getEventTime() * 1000L + 5000L);

                }


            }

            //专门针对receiptEvent来进行解析
            @Override
            public void processElement2(ReceiptEvent receiptEvent, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {

                OrderEvent orderEvent = orderEventValueState.value();
                //对账流将数据已经到了
                if (orderEvent != null) {

                    Tuple2<OrderEvent, ReceiptEvent> orderEventReceiptEventTuple2 = new Tuple2<>();
                    orderEventReceiptEventTuple2.setFields(orderEvent, receiptEvent);

                    orderEventValueState.clear();

                } else {
                    //对账流数据还没到
                    receiptEventValueState.update(receiptEvent);
                    ctx.timerService().registerEventTimeTimer(receiptEvent.getEventTime() * 1000L + 5000L);

                }


            }

            //定时任务方法，用于定时的去清理state的数据
            @Override
            public void onTimer(long timestamp, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
                if (orderEventValueState.value() != null) {
                    ctx.output(unmatchedPays, orderEventValueState.value());
                }

                if (receiptEventValueState.value() != null) {
                    ctx.output(unmatchedReceipts, receiptEventValueState.value());
                }

            }
        });


        processStream.print("matched");
        processStream.getSideOutput(unmatchedReceipts).print("unmatchedRecepites");
        processStream.getSideOutput(unmatchedPays).print("unmatchedPays");

        executionEnvironment.execute();



    }


}
