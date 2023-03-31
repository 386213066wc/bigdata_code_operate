package cn.flink.demo20;


import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * todo: 基于Flink CEP进行订单超时检测
 */
public class CheckOrderTimeoutWithCepByJava {
    private   static FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        //todo: 1、构建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //todo: 2、接受socket数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("bigdata01", 9999);

        //todo: 3、数据处理分组
        KeyedStream<OrderDetail, Tuple> keyedStream = socketTextStream.map(new MapFunction<String, OrderDetail>() {
            public OrderDetail map(String value) throws Exception {
                String[] strings = value.split(",");
                return new OrderDetail(strings[0], strings[1], strings[2], Double.parseDouble(strings[3]));
            }
            //添加watermark
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                //指定对应的TimestampAssigner
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        //指定EventTime对应的字段
                        long extractTimestamp = 0;
                        try {
                            extractTimestamp = fastDateFormat.parse(element.orderCreateTime).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return extractTimestamp;
                    }
                })).keyBy("orderId");

        //todo: 4、定义Parttern
        Pattern<OrderDetail, OrderDetail> pattern = Pattern.<OrderDetail>begin("start").where(new SimpleCondition<OrderDetail>() {
                    @Override
                    public boolean filter(OrderDetail orderDetail) throws Exception {

                        //第一个装填必须是1  表示下单
                        if (orderDetail.status.equals("1")) {
                            return true;
                        }
                        return false;
                    }
                })
                .followedBy("second").where(new SimpleCondition<OrderDetail>() {
                    //紧接着 订单的第二个状态必须是2
                    @Override
                    public boolean filter(OrderDetail orderDetail) throws Exception {

                        if (orderDetail.status.equals("2")) {
                            return true;
                        }
                        return false;
                    }
                    //指定有效的时间约束
                }).within(Time.seconds(10)); //在多长时间内没有出现2这个状态，就算作超时

        //todo: 5、将Parttern应用到事件流中进行检测，同时指定时间类型
        PatternStream<OrderDetail> patternStream = CEP.pattern(keyedStream, pattern).inEventTime();


        //todo: 6、提取匹配到的数据
        //定义侧输出流标签，存储超时的订单数据
        OutputTag outputTag = new OutputTag("timeout", TypeInformation.of(OrderDetail.class));

        SingleOutputStreamOperator<OrderDetail> result = patternStream.select(outputTag, new MyPatternTimeoutFunction(), new MyPatternSelectFunction());

        result.print("支付成功的订单：");

        result.getSideOutput(outputTag).print("超时的订单：");

        //todo: 7、提交任务
        env.execute("CheckOrderTimeoutWithCepByJava");

    }

    //todo: 提取正常支付成功的订单数据
    static class MyPatternSelectFunction implements PatternSelectFunction<OrderDetail,OrderDetail> {

        @Override
        public OrderDetail select(Map<String, List<OrderDetail>> patternMap) throws Exception {

            List<OrderDetail> secondOrderDetails = patternMap.get("second");
            //支付成功的订单
            OrderDetail orderDetailSuccess = secondOrderDetails.iterator().next();
            //返回
            return orderDetailSuccess;
        }
    }

    //todo: 提取超时的订单数据
    static class MyPatternTimeoutFunction implements PatternTimeoutFunction<OrderDetail,OrderDetail> {
        @Override
        public OrderDetail timeout(Map<String, List<OrderDetail>> patternMap, long timeoutTimestamp) throws Exception {

            List<OrderDetail> startTimeoutOrderDetails = patternMap.get("start");
            //超时订单
            OrderDetail orderDetailTimeout = startTimeoutOrderDetails.iterator().next();
            //返回
            return orderDetailTimeout;

        }
    }

    //todo:定义订单信息实体类
    public static class OrderDetail{
        //订单编号
        public String orderId;
        //订单状态
        public String status;
        //下单时间
        public String orderCreateTime;
        //订单金额
        public Double price;

        //无参构造必须带上
        public OrderDetail() {
        }

        public OrderDetail(String orderId, String status, String orderCreateTime, Double price) {
            this.orderId = orderId;
            this.status = status;
            this.orderCreateTime = orderCreateTime;
            this.price = price;
        }

        @Override
        public String toString() {
            return orderId+"\t"+status+"\t"+orderCreateTime+"\t"+price;
        }
    }
}