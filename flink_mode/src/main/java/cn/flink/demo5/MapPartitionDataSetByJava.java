package cn.flink.demo5;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class MapPartitionDataSetByJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> stringDataSource = executionEnvironment.fromElements("hello world1", "hello world2", "hello world3", "hello world4", "hello world5", "hello world6");

        MapPartitionOperator<String, String> eachLine = stringDataSource.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                Iterator<String> eachPartitionValues = values.iterator();
                while (eachPartitionValues.hasNext()) {
                    String eachLine = eachPartitionValues.next() + "mapPartitionOpt";
                    out.collect(eachLine);
                }
            }
        });

        eachLine.print();
    }
}