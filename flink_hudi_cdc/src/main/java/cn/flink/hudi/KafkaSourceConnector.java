package cn.flink.hudi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class KafkaSourceConnector {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //构建运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        environment.enableCheckpointing(3000);

        //设置checkPoint一致性语义
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置任务关闭的时候保留最后一次的checkPoint 主要是为了下次我们重启程序，可以checkPoint或者savePoint里面重新启动，实现断点续传
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置checkpoint重启策略
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));
        //设置状态保存位置  也可以保存在rocksDB，
        environment.setStateBackend(new FsStateBackend("hdfs://bigdata01:8020/flinkCDC"));

        //设置hadoop用户名
        System.setProperty("HADOOP_USER_NAME","hadoop");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("bigdata01:9092,bigdata02:9092,bigdata03:9092")
                .setTopics("kafka_cdc")
                .setGroupId("my-group")
                //设置动态分区发现，如果新增分区，自动发现
                .setProperty("partition.discovery.interval.ms", "10000")
                //设置offset消费的位置
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        environment.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(1)
                .print();
        environment.execute();
    }
}