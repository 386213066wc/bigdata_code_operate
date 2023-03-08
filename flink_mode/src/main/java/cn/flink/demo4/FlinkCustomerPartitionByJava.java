package cn.flink.demo4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;
import java.util.UUID;

public class FlinkCustomerPartitionByJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> sourceStream = executionEnvironment.fromElements("hello world", "hello spark", "hudi hive", "test result");

        DataStream<String> customPartition = sourceStream.partitionCustom(new MyPartitioner(), new KeySelector<String, String>() {

            @Override
            public String getKey(String value) throws Exception {
                return value + "";
            }
        });


        SingleOutputStreamOperator<String> mapStream = customPartition.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("当前线程ID为" + Thread.currentThread().getId());
                return value;
            }
        });

        mapStream.print();
        executionEnvironment.execute();
    }
}
class MyPartitioner  implements Partitioner<String> {
    //覆写partition方法，进行自定义分区
    @Override
    public int partition(String key, int numPartitions) {
        System.out.println(numPartitions);
      /* int result =  (key.hashCode()&Integer.MAX_VALUE) % numPartitions =5 ;
       return result;*/
        if(key.contains("hello")){
            return 0;
        }
        return 1;
    }
}