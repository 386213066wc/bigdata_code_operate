package cn.flink.demo5;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class CrossJoinByJava {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        Tuple2<Integer, String> tuple1 = new Tuple2<Integer, String>();
        tuple1.setFields(1, "zhangsan");
        Tuple2<Integer, String> tuple2 = new Tuple2<Integer, String>();
        tuple2.setFields(2, "lisi");
        DataSource<Tuple2<Integer, String>> sourceStream1 = executionEnvironment.fromElements(tuple1, tuple2);

        Tuple2<Integer, String> tuple3 = new Tuple2<Integer, String>();
        tuple3.setFields(1, "80");
        Tuple2<Integer, String> tuple4 = new Tuple2<Integer, String>();
        tuple4.setFields(4, "80");

        DataSource<Tuple2<Integer, String>> sourceStream2 = executionEnvironment.fromElements(tuple3, tuple4);


        CrossOperator.DefaultCross<Tuple2<Integer, String>, Tuple2<Integer, String>> crossJoin = sourceStream1.cross(sourceStream2);

        long countResult = crossJoin.count();
        crossJoin.print();
        System.out.println(countResult);


    }
}