package cn.flink.demo5;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.join.JoinFunctionAssigner;
import org.apache.flink.api.java.operators.join.JoinOperatorSetsBase;
import org.apache.flink.api.java.tuple.Tuple2;

public class LeftJoinAndRightJoinByJava {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        Tuple2<Integer, String> tuple1 = new Tuple2<Integer,String>();
        tuple1.setFields(1,"zhangsan");
        Tuple2<Integer, String> tuple2 = new Tuple2<Integer,String>();
        tuple2.setFields(2,"lisi");
        DataSource<Tuple2<Integer, String>> sourceStream1 = executionEnvironment.fromElements(tuple1,tuple2);

        Tuple2<Integer, String> tuple3 = new Tuple2<Integer,String>();
        tuple3.setFields(1,"80");
        Tuple2<Integer, String> tuple4 = new Tuple2<Integer,String>();
        tuple4.setFields(4,"80");

        DataSource<Tuple2<Integer, String>> sourceStream2 = executionEnvironment.fromElements(tuple3,tuple4);




        //使用left join进行数据的join操作
        JoinOperatorSetsBase<Tuple2<Integer, String>, Tuple2<Integer, String>> leftOuterJoin = sourceStream1.leftOuterJoin(sourceStream2);

        JoinFunctionAssigner<Tuple2<Integer, String>, Tuple2<Integer, String>> joinStream = leftOuterJoin.where(new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        }).equalTo(new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        });

        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, String> joinOperator = joinStream.with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
            @Override
            public String join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if(null !=  second){
                    return first.f0 + first.f1 + second.f1;
                }else{
                    return first.f0 + first.f1 + "null";
                }

            }
        });

        joinOperator.print();


        JoinOperatorSetsBase<Tuple2<Integer, String>, Tuple2<Integer, String>> rightOuterJoin = sourceStream1.rightOuterJoin(sourceStream2);

        JoinFunctionAssigner<Tuple2<Integer, String>, Tuple2<Integer, String>> joinedStream = rightOuterJoin.where(new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        }).equalTo(new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {

                return value.f0;
            }
        });

        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, String> joinOperator1 = joinedStream.with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
            @Override
            public String join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if(null  != first){
                    return   first.f0 + first.f1 + second.f1;
                }else{
                    return "null" + second.f0  + second.f1;
                }

            }
        });

        joinOperator1.print();


    }
}