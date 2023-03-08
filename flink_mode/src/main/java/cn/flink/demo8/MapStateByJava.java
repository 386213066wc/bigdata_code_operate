package cn.flink.demo8;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.UUID;

public class MapStateByJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        Tuple2<Integer, Double> tuple1 = new Tuple2<>();
        tuple1.setFields(1,1d);

        Tuple2<Integer, Double> tuple2 = new Tuple2<>();
        tuple2.setFields(1,2d);


        Tuple2<Integer, Double> tuple3 = new Tuple2<>();
        tuple3.setFields(1,3d);


        Tuple2<Integer, Double> tuple4 = new Tuple2<>();
        tuple4.setFields(1,4d);

        Tuple2<Integer, Double> tuple5 = new Tuple2<>();
        tuple5.setFields(1,5d);

        Tuple2<Integer, Double> tuple6 = new Tuple2<>();
        tuple6.setFields(2,1d);

        Tuple2<Integer, Double> tuple7 = new Tuple2<>();
        tuple7.setFields(2,2d);


        Tuple2<Integer, Double> tuple8 = new Tuple2<>();
        tuple8.setFields(2,3d);


        Tuple2<Integer, Double> tuple9 = new Tuple2<>();
        tuple9.setFields(2,4d);

        Tuple2<Integer, Double> tuple10 = new Tuple2<>();
        tuple10.setFields(2,5d);



        DataStreamSource<Tuple2<Integer, Double>> tuple2DataStreamSource = executionEnvironment.fromElements(
                tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8, tuple9, tuple10);

        KeyedStream<Tuple2<Integer, Double>, Integer> tuple2IntegerKeyedStream = tuple2DataStreamSource.keyBy(new KeySelector<Tuple2<Integer, Double>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, Double> value) throws Exception {
                return value.f0;
            }
        });

        tuple2IntegerKeyedStream.flatMap(new RichFlatMapFunction<Tuple2<Integer, Double>, AvgValue>() {
            private MapState<String,AvgValue> mapState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, AvgValue> descriptor = new MapStateDescriptor<String,AvgValue>("mapState", String.class, AvgValue.class);
                mapState =    getRuntimeContext().getMapState(descriptor);
            }

            @Override
            public void flatMap(Tuple2<Integer, Double> value, Collector<AvgValue> out) throws Exception {
                //Integer num, Double sumValue, Integer countNum
                AvgValue avgValue = new AvgValue(value.f0, value.f1, 1);
                mapState.put(UUID.randomUUID().toString(),avgValue);

                //获取迭代器
                Iterator<AvgValue> iterator = mapState.values().iterator();

                Double sumValue = 0d;
                Integer countNum = 0;
                Double avgVal = 0d;

                while(iterator.hasNext()){
                    AvgValue next = iterator.next();
                    sumValue +=  next.getSumValue();
                    countNum += next.getCountNum();
                    avgVal = sumValue/countNum;
                }

                AvgValue resultAvg = new AvgValue(value.f0, sumValue, countNum);
                resultAvg.setAvgValue(avgVal);

                mapState.clear();
                mapState.put(UUID.randomUUID().toString(),resultAvg);
                out.collect(resultAvg);
            }
        }).print();

        executionEnvironment.execute();
    }
}