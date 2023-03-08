package cn.flink.demo8;

import lombok.val;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class ValueStateOperateByJava {

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


        DataStreamSource<Tuple2<Integer, Double>> tuple2DataStreamSource = executionEnvironment.fromElements(tuple1, tuple2, tuple3, tuple4, tuple5);

        SingleOutputStreamOperator<AvgValue> avgValue = tuple2DataStreamSource.keyBy(new KeySelector<Tuple2<Integer, Double>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, Double> value) throws Exception {

                return value.f0;
            }
        }).flatMap(new RichFlatMapFunction<Tuple2<Integer, Double>, AvgValue>() {

            //通过open方法获取状态
            private ValueState<AvgValue> valueState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<AvgValue> avgValueValueStateDescriptor = new ValueStateDescriptor<AvgValue>("avgValue",AvgValue.class);

                this.valueState = getRuntimeContext().getState(avgValueValueStateDescriptor);

            }

            @Override
            public void flatMap(Tuple2<Integer, Double> value, Collector<AvgValue> out) throws Exception {
                //获取到了状态
                AvgValue firstValue = valueState.value();
                if(null == firstValue ){
                    firstValue = new AvgValue(0, 0d, 0);
                }

                Integer eachKey = value.f0;
                Double addValue = value.f1;


                firstValue.setCountNum(firstValue.getCountNum() + 1);
                firstValue.setSumValue(firstValue.getSumValue() + addValue);
                firstValue.setNum(eachKey);


                //计算平均值
                firstValue.setAvgValue(firstValue.getSumValue() / firstValue.getCountNum());
                //进行更新状态
                valueState.update(firstValue);
                out.collect(firstValue);

            }
        });
        avgValue.print();
        executionEnvironment.execute();



    }

}



class AvgValue implements Serializable {
    //定义了几个字段  num 表示是数字
    //
    private Integer num;
    //表示数据累加的综合
    private Double sumValue;
    //一共传入了多少个数据
    private Integer countNum;

    //最终的平均值是多少
    private Double avgValue;

    public AvgValue() {
    }

    public AvgValue(Integer num, Double sumValue, Integer countNum) {
        this.num = num;
        this.sumValue = sumValue;
        this.countNum = countNum;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public Double getSumValue() {
        return sumValue;
    }

    public void setSumValue(Double sumValue) {
        this.sumValue = sumValue;
    }

    public Integer getCountNum() {
        return countNum;
    }

    public void setCountNum(Integer countNum) {
        this.countNum = countNum;
    }

    public Double getAvgValue() {
        return avgValue;
    }

    public void setAvgValue(Double avgValue) {
        this.avgValue = avgValue;
    }

    @Override
    public String toString() {
        return "AvgValue{" +
                "num=" + num +
                ", sumValue=" + sumValue +
                ", countNum=" + countNum +
                ", avgValue=" + avgValue +
                '}';
    }
}

