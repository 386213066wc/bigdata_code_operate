package cn.flink.demo8;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OperatorSinkByJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        //设置memory state backend
        executionEnvironment.setStateBackend(new MemoryStateBackend());
        //设置fs  state backedn
        executionEnvironment.setStateBackend(new FsStateBackend("hdfs://bigdata01:8020/flink/checkDir"));


       // StateBackendFactory
     //   executionEnvironment.setStateBackend()

        //设置rocksDB 的state backend
        executionEnvironment.setStateBackend(new RocksDBStateBackend("hdfs://bigdata01:8020/flink_rocksdb/backend"));

        /**
         * 配置checkPoint的操作
         *
         */


        //默认checkpoint功能是disabled的，想要使用的时候需要先启用
// 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
        //实际工作当中，checkpoint的间隔时间不宜太短，一般设置10分钟左右
        executionEnvironment.enableCheckpointing(5000);
// 高级选项：
// 设置模式为exactly-once （这是默认值）
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
// 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(60000);
// 同一时间只允许进行一个检查点
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
// 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】

/**
 * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
 * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
 */
        executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //   executionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //允许使用checkpoint非对齐检查点
        executionEnvironment.getCheckpointConfig().enableUnalignedCheckpoints();




        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("bigdata01", 9999);

        SingleOutputStreamOperator<String> result = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).uid("hellworold");
        result.addSink(new MySinkFunction());
        executionEnvironment.execute();

    }


}

class MySinkFunction implements SinkFunction<String>, CheckpointedFunction {

    private int threadHold = 2;
    private List<String> list = new ArrayList<String>();

    private ListState<String> checkPointState = null;

    @Override
    public void invoke(String value, Context context) throws Exception {
        list.add(value);
        if(list.size() == threadHold){
            System.out.println("两个元素开始打印" + list.get(0) + "\t" + list.get(1));
            list.clear();
        }
    }

    //对operator state进行快照的保存
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkPointState.clear();
        for (String s : list) {
            checkPointState.add(s);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkPointState = context.getOperatorStateStore().getListState(new ListStateDescriptor<String>("operatorState", String.class));
        if (context.isRestored()) {
            Iterator<String> iterator = checkPointState.get().iterator();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
        }



    }
}