package cn.kafka.opt;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class ConsumPartition {
    public static void main(String[] args) {
        Properties props= new Properties();
        props.put("bootstrap.servers","bigdata01:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        String topic ="foo";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        //


       /*
        订阅某些topic进行消费
        consumer.subscribe();
        //指定partition，指定的offset进行消费
        指定消费partition2 对应的offset的值5000
        consumer.seek();
        //指定partition来进行消费
        consumer.assign();*/

        consumer.assign(Arrays.asList(partition0, partition1));
//手动指定消费指定分区的数据---end
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record : records)
                System.out.printf("offset= %d, key = %s, value = %s%n", record.offset(), record.key(),record.value());
        }
    }
}