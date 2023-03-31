package cn.flink.demo10;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import java.util.Properties;

/**
 *
 * 实现思想
 * 这里简单说下这个类的作用就是实现这个类的方法：beginTransaction、preCommit、commit、abort，达到事件（preCommit）预提交的逻辑（当事件进行自己的逻辑处理后进行预提交，
 * 如果预提交成功之后才进行真正的（commit）提交，如果预提交失败则调用abort方法进行事件的回滚操作），结合flink的checkpoint机制，来保存topic中partition的offset。
 *
 * 达到的效果我举个例子来说明下：比如checkpoint每10s进行一次，此时用FlinkKafkaConsumer实时消费kafka中的消息，消费并处理完消息后，进行一次预提交数据库的操作，
 * 如果预提交没有问题，10s后进行真正的插入数据库操作，如果插入成功，进行一次checkpoint，flink会自动记录消费的offset，
 * 可以将checkpoint保存的数据放到hdfs中，如果预提交出错，比如在5s的时候出错了，
 * 此时Flink程序就会进入不断的重启中，重启的策略可以在配置中设置，当然下一次的checkpoint也不会做了，
 * checkpoint记录的还是上一次成功消费的offset，本次消费的数据因为在checkpoint期间，消费成功，
 * 但是预提交过程中失败了，注意此时数据并没有真正的执行插入操作，因为预提交（preCommit）失败，提交（commit）过程也不会发生了。
 * 等你将异常数据处理完成之后，再重新启动这个Flink程序，它会自动从上一次成功的checkpoint中继续消费数据，以此来达到Kafka到Mysql的Exactly-Once。
 *
 *
 */


//todo: 消费kafka的topic数据，实现结果写入到mysql表中，实现端到端的exactly-once语义
//保证kafka to mysql的Exactly-Once
public class TwoPhaseCommitKafka2Mysql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //flink当中，默认的就是使用eventTime
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        environment.setParallelism(1);
        //默认checkpoint功能是disabled的，想要使用的时候需要先启用
        // 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
        environment.enableCheckpointing(5000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        environment.getCheckpointConfig().setCheckpointTimeout(5000);
        // 同一时间只允许进行一个检查点
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】

        /**
         * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
         * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
         */
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        environment.setStateBackend(new FsStateBackend("hdfs://bigdata01:8020/kafka2mysql/checkpoints",true));

        String topic = "kafka2mysql";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","bigdata01:9092,bigdata02:9092,bigdata03:9092");
        prop.setProperty("group.id","flink-consumer");
        //flink开启自动检测kafkatopic新增的分区机制
        prop.setProperty("flink.partition-discovery.interval-millis","3000");
        //SimpleStringSchema可以获取到kafka消息，
        // JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息

        FlinkKafkaConsumer<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer<ObjectNode>(topic,new JSONKeyValueDeserializationSchema(true),prop);


        //把消息的偏移量通过checkpoint机制保存在HDFS上
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        //todo: 加载topic数据
        DataStreamSource<ObjectNode> kafkaSource = environment.addSource(kafkaConsumer);
        kafkaSource.print("kafka topic data -----> ");

        //todo: 数据写入到mysql表中
        kafkaSource.addSink(new MySqlTwoPhaseCommitSink());

        //todo: 触发计算
        environment.execute("TwoPhaseCommitKafka2Mysql");
    }
}