package cn.flink.demo8;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class AggregateStateByJava {
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

        tuple2IntegerKeyedStream.flatMap(new AggregrateRichFlatMap()).print();

        executionEnvironment.execute();
    }
}


class AggregrateRichFlatMap extends RichFlatMapFunction<Tuple2<Integer,Double>,Tuple2<Integer,String>>{

    private AggregatingState<Integer,String> aggregatingState;


    @Override
    public void open(Configuration parameters) throws Exception {


        AggregatingStateDescriptor<Integer, String, String> aggregateStateDescriptor = new AggregatingStateDescriptor<>("aggregateState", new AggregateFunction<Integer, String, String>() {
            /**
             * 表示字符串连接的初始化的值
             * @return
             */
            @Override
            public String createAccumulator() {
                return "contains";
            }

            @Override
            public String add(Integer value, String accumulator) {
                return accumulator + "-" + value;
            }

            @Override
            public String getResult(String accumulator) {
                return accumulator;
            }

            /**
             * 第一个字符串跟第二个字符串累加的方式，使用 - 来进行连接
             * @param a
             * @param b
             * @return
             */
            @Override
            public String merge(String a, String b) {
                return a + "-" + b;
            }
        }, String.class);
        aggregatingState =   getRuntimeContext().getAggregatingState(aggregateStateDescriptor);
    }
    @Override
    public void flatMap(Tuple2<Integer, Double> value, Collector<Tuple2<Integer, String>> out) throws Exception {
        aggregatingState.add(value.f0);
        //  out.collect(value.f0,aggregatingState.get()+"");\
        Tuple2<Integer, String> tuple2 = new Tuple2<>();
        tuple2.setFields(value.f0,aggregatingState.get());
        out.collect(tuple2);

    }
}