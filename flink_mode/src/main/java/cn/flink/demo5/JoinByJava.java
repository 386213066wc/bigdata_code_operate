package cn.flink.demo5;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;

public class JoinByJava {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        Tuple2<Integer, String> tuple1 = new Tuple2<Integer,String>();
        tuple1.setFields(1,"zhangsan");
        DataSource<Tuple2<Integer, String>> sourceStream1 = executionEnvironment.fromElements(tuple1);

        Tuple2<Integer, String> tuple2 = new Tuple2<Integer,String>();
        tuple2.setFields(1,"80");
        DataSource<Tuple2<Integer, String>> sourceStream2 = executionEnvironment.fromElements(tuple2);


        //  select from user  join order on user.id = order.oid
        JoinOperator.JoinOperatorSets<Tuple2<Integer, String>, Tuple2<Integer, String>> joinStream = sourceStream1.join(sourceStream2);

        JoinOperator.JoinOperatorSets<Tuple2<Integer, String>, Tuple2<Integer, String>>.JoinOperatorSetsPredicate whereStream = joinStream
                .where(new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        });
        JoinOperator.DefaultJoin<Tuple2<Integer, String>, Tuple2<Integer, String>> secondStream = whereStream.equalTo(new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        });

        MapOperator<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, String> joinLine = secondStream.map(new MapFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, String>() {
            @Override
            public String map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                String line = value.f0.f0 + value.f0.f1 + value.f1.f1;
                return line;
            }
        });
        joinLine.print();
    }
}