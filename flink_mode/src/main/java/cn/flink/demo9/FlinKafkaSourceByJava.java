package cn.flink.demo9;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinKafkaSourceByJava {
    //消费kafka的数据，一定要设置checkpoint
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //默认checkpoint功能是disabled的，想要使用的时候需要先启用
// 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
        executionEnvironment.enableCheckpointing(5000);

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
        executionEnvironment.getCheckpointConfig().setCheckpointStorage(new Path("hdfs://bigdata01:8020/flink_kafka_check"));
// 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】

/**
 * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
 * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
 */
        executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //   executionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //允许使用checkpoint非对齐检查点
        executionEnvironment.getCheckpointConfig().enableUnalignedCheckpoints();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("bigdata01:9092,bigdata02:9092,bigdata03:9092")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setTopics("test")
                .setGroupId("test_group_id")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        DataStreamSource<String> dataStreamSource = executionEnvironment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        SingleOutputStreamOperator<String> eachWord = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(s1);
                }
            }
        });

        eachWord.print();

        executionEnvironment.execute();


    }


}
