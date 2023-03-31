package cn.flink.demo20;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.text.ParseException;
import java.util.List;
import java.util.Map;


/**
 * todo: 基于Flink CEP进行设备温度异常检测
 */
public class CheckTempeatureWithCepByJava {

    private   static  FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        //todo: 1、构建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //todo: 2、接受socket数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("bigdata01", 9999);

        //todo: 3、数据处理分组
        KeyedStream<DeviceDetail, Tuple> keyedStream = socketTextStream.map(new MapFunction<String, DeviceDetail>() {

            public DeviceDetail map(String value) throws Exception {
                String[] strings = value.split(",");
                return new DeviceDetail(strings[0], strings[1], strings[2], strings[3],strings[4],strings[5]);
            }
            //基于有序的数据流生成watermark
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<DeviceDetail>forMonotonousTimestamps()
                // 必须指定对应的timeStamp
                .withTimestampAssigner(new SerializableTimestampAssigner<DeviceDetail>() {
                    @Override
                    public long extractTimestamp(DeviceDetail element, long recordTimestamp) {
                        //指定EventTime对应的字段
                        long extractTimestamp = 0;
                        try {
                            extractTimestamp = fastDateFormat.parse(element.createtime).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return extractTimestamp;

                    }
                })
        ).keyBy("deviceMac");


        //todo: 4、定义Parttern
        Pattern<DeviceDetail, DeviceDetail> pattern = Pattern.<DeviceDetail>begin("start").where(new SimpleCondition<DeviceDetail>() {
                    @Override
                    public boolean filter(DeviceDetail deviceDetail) throws Exception {
                        if (Integer.parseInt(deviceDetail.temperature) >= 40) {
                            return true;
                        }
                        return false;
                    }
                })
                .followedByAny("second").where(new SimpleCondition<DeviceDetail>() {
                    @Override
                    public boolean filter(DeviceDetail deviceDetail) throws Exception {
                        if (Integer.parseInt(deviceDetail.temperature) >= 40) {
                            return true;
                        }
                        return false;
                    }
                })
                .followedByAny("three").where(new SimpleCondition<DeviceDetail>() {
                    @Override
                    public boolean filter(DeviceDetail deviceDetail) throws Exception {
                        if (Integer.parseInt(deviceDetail.temperature) >= 40) {
                            return true;
                        }
                        return false;
                    }
                })//指定有效的时间约束
                .within(Time.seconds(10));

        //todo: 5、将Parttern应用到事件流中进行检测，同时指定时间类型
        PatternStream<DeviceDetail> deviceDetailPatternStream = CEP.pattern(keyedStream, pattern).inEventTime();

        //todo: 6、提取匹配到的数据
        SingleOutputStreamOperator<Void> selectStream = deviceDetailPatternStream.flatSelect(new PatternFlatSelectFunction<DeviceDetail, Void>() {
            @Override
            public void flatSelect(Map<String, List<DeviceDetail>> patternMap, Collector<Void> out) throws Exception {

                //map的key有三种情况：start、second、three
                //todo: 获取每个模式匹配到的数据集
                List<DeviceDetail> startMatchList = patternMap.get("start");
                List<DeviceDetail> secondMatchList = patternMap.get("second");
                List<DeviceDetail> threeMatchList = patternMap.get("three");

                //todo: 分别打印
                DeviceDetail startResult = startMatchList.iterator().next();
                DeviceDetail secondResult = secondMatchList.iterator().next();
                DeviceDetail threeResult = threeMatchList.iterator().next();

                System.out.println("第一条数据: " + startResult);
                System.out.println("第二条数据: " + secondResult);
                System.out.println("第三条数据: " + threeResult);

            }
        });

        //todo: 7、提交任务
        env.execute("CheckTempeatureWithCepByJava");

    }


    //todo:定义温度信息实体类
    public static   class  DeviceDetail {
        //传感器设备mac地址
        public  String sensorMac;
        //检测机器mac地址
        public  String deviceMac;
        //温度
        public  String temperature;
        //湿度
        public  String dampness;
        //气压
        public  String pressure;
        //数据产生时间
        public  String createtime;

        //无参构造必须带上
        public DeviceDetail() {}

        public DeviceDetail(String sensorMac, String deviceMac, String temperature, String dampness, String pressure, String createtime) {
            this.sensorMac = sensorMac;
            this.deviceMac = deviceMac;
            this.temperature = temperature;
            this.dampness = dampness;
            this.pressure = pressure;
            this.createtime = createtime;
        }

        @Override
        public String toString() {
            return sensorMac+"\t"+deviceMac+"\t"+temperature+"\t"+dampness+"\t"+pressure+"\t"+createtime;
        }
    }
}