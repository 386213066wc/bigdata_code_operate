package cn.flink.demo7;


import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;

public class FlinkDistributeCacheByJava {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        //
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.registerCachedFile("E:\\1、课程内容\\1、课程资料\\11、Flink实时处理\\4、数据集\\catalina.out","student");

        Tuple3<Integer, String, Integer> tuple3 = new Tuple3<>();
        tuple3.setFields(1,"语文",60);


        Tuple3<Integer, String, Integer> tuple4 = new Tuple3<>();
        tuple4.setFields(2,"数学",80);


        DataSource<Tuple3<Integer, String, Integer>> sourceDataset = executionEnvironment.fromElements(tuple3, tuple4);

        MapOperator<Tuple3<Integer, String, Integer>, String> result = sourceDataset.map(new RichMapFunction<Tuple3<Integer, String, Integer>, String>() {
            private List<String> fileLines;
            private Counter counter ;
            private IntCounter intCounter  = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                //定义累加器
                getRuntimeContext().addAccumulator("intCounter",intCounter);
                //定义计数器，来一次累加一次
                this.counter = getRuntimeContext().getMetricGroup().counter("counter");
                //分布式缓存
                File student = getRuntimeContext().getDistributedCache().getFile("student");
                this.fileLines = FileUtils.readLines(student, "UTF-8");
            }

            @Override
            public String map(Tuple3<Integer, String, Integer> value) throws Exception {
                for (String fileLine : fileLines) {
                    //操作累加器
                    intCounter.add(10);
                    //操作计数器
                    counter.inc();
                }
                return value.f0 + "\t" + value.f1 + "\t" + value.f2;
            }
        });
        result.print();
        JobExecutionResult lastJobExecutionResult = executionEnvironment.getLastJobExecutionResult();
        //获取累加器的结果值
        Object intCounter = lastJobExecutionResult.getAccumulatorResult("intCounter");
        System.out.println(intCounter.toString());
    }
}