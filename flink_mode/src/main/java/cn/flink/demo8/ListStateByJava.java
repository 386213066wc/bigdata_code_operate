package cn.flink.demo8;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ListStateByJava {
    public static void main(String[] args) throws Exception {
        //获取程序的入口类
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

        //按照数据的key进行分组
        KeyedStream<Tuple2<Integer, Double>, Integer> tuple2IntegerKeyedStream = tuple2DataStreamSource.keyBy(new KeySelector<Tuple2<Integer, Double>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, Double> value) throws Exception {
                return value.f0;
            }
        });
        SingleOutputStreamOperator<AvgValue> result = tuple2IntegerKeyedStream.flatMap(new RichFlatMapFunction<Tuple2<Integer, Double>, AvgValue>() {

            private ListState<AvgValue> listState;


            /**
             * 初始化的方法，只会调用一次
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<AvgValue> listStateDescriptor = new ListStateDescriptor<>("listState", AvgValue.class);
             //   getRuntimeContext().getState()
                listState = getRuntimeContext().getListState(listStateDescriptor);
            }

            @Override
            public void flatMap(Tuple2<Integer, Double> value, Collector<AvgValue> out) throws Exception {
                //Integer num, Double sumValue, Integer countNum
                //每次来的数据，先添加到listState里面去
                AvgValue avgValue1 = new AvgValue(value.f0, value.f1, 1);
                listState.add(avgValue1);

                //获取到listState里面所有的数据来求取平均值
                Iterator<AvgValue> iterator = listState.get().iterator();
             /*   Integer eachKey = value.f0;
                Double addValue = value.f1;*/

                List<AvgValue> avgValueList = new ArrayList<AvgValue>();
                Integer countNum = 0;
                Double sumValue = 0d;
                Double avgVal = 0d;
                //循环遍历listState当中每一个数据，然后求取平均值
                while(iterator.hasNext()){
                    AvgValue next = iterator.next();
                    countNum = countNum +  next.getCountNum()  ;
                    sumValue = sumValue +  next.getSumValue();
                    avgVal = sumValue/countNum;
                }

                //求取出来的平均值结果，重新存放到ListState里面去
                AvgValue avgValue2 = new AvgValue(value.f0, sumValue, countNum);
                avgValue2.setAvgValue(avgVal);
                //清空状态
                listState.clear();
                avgValueList.add(avgValue2);
                //更新状态
                listState.update(avgValueList);
                //将求取出来的平均值结果给发送出去，便于下一条数据来了，继续求取平均值结果
                out.collect(avgValue2);
            }
        });
        result.print();
        executionEnvironment.execute();
    }
}