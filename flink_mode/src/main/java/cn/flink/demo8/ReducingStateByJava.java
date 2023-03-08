package cn.flink.demo8;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReducingStateByJava {

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
        tuple2IntegerKeyedStream.flatMap(new RichFlatMapFunction<Tuple2<Integer, Double>, Tuple2<Integer,Double>>() {
            private ReducingState<Double> reducingState = null;
            private Long counter = 0L;
            @Override
            public void open(Configuration parameters) throws Exception {
                ReducingStateDescriptor<Double> reducSum = new ReducingStateDescriptor<>("reducSum", new ReduceFunction<Double>() {
                    /**
                     *
                     * @param value1  传入的数据值
                     * @param value2  每次累加之后的结果数据值
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public Double reduce(Double value1, Double value2) throws Exception {
                        return value1 + value2;
                    }
                }, Double.class);
                reducingState =  getRuntimeContext().getReducingState(reducSum);
            }
            @Override
            public void flatMap(Tuple2<Integer, Double> value, Collector<Tuple2<Integer, Double>> out) throws Exception {
                counter +=1 ;
                reducingState.add(value.f1);
                Tuple2<Integer, Double> longDoubleTuple2 = new Tuple2<>();
                longDoubleTuple2.setFields(value.f0,reducingState.get()/counter);
                out.collect(longDoubleTuple2);
            }
        }).print();
        executionEnvironment.execute();
    }
}