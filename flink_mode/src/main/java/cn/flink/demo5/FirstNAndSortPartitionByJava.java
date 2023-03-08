package cn.flink.demo5;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

public class FirstNAndSortPartitionByJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        Tuple3<Integer, String,Integer> tuple1 = new Tuple3<Integer,String,Integer>();
        tuple1.setFields(1,"zhangsan",20);

        Tuple3<Integer, String,Integer> tuple2 = new Tuple3<Integer,String,Integer>();
        tuple2.setFields(2,"lisi",30);

        Tuple3<Integer, String,Integer> tuple3 = new Tuple3<Integer,String,Integer>();
        tuple3.setFields(3,"wangwu",50);

        Tuple3<Integer, String,Integer> tuple4 = new Tuple3<Integer,String,Integer>();
        tuple4.setFields(4,"zhaoliu",40);

        Tuple3<Integer, String,Integer> tuple5 = new Tuple3<Integer,String,Integer>();
        tuple5.setFields(5,"zhaoliu",30);

        DataSource<Tuple3<Integer, String, Integer>> tupleSource = executionEnvironment.fromElements(tuple1, tuple2, tuple3, tuple4,tuple5);

        tupleSource.first(2).print();

        tupleSource.groupBy(1).sortGroup(2, Order.DESCENDING)
                .first(1).print();
    }
}