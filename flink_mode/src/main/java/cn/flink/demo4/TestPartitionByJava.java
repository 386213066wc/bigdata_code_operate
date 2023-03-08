package cn.flink.demo4;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestPartitionByJava {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> sourceStream = executionEnvironment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 34);

        //调用了filter之后，数据量可能就会大幅度减少   分区数也可以减少
        SingleOutputStreamOperator<Integer> filterStream = sourceStream.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                if (value < 10) {
                    return true;
                } else {
                    return false;
                }
            }
        })/*.setParallelism(8)*/ /* .rescale()*/ /*.shuffle()*//*.shuffle()*/;
       //    .shuffle();  //随机的重新分发数据,上游的数据，随机的发送到下游的分区里面去
        //.rebalance()  //对数据重新进行分区，涉及到shuffle的过程
        //     .rescale()    //跟rebalance有点类似，但不是全局的，这种方式仅发生在一个单一的节点，因此没有跨网络的数据传输。

        filterStream.print();
        executionEnvironment.execute();
    }
}
