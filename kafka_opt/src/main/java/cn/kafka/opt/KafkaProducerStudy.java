package cn.kafka.opt;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerStudy {
    /**
     * 通过javaAPI实现向kafka当中生产数据
     * @param args
     */
    public static void main(String[] args) {


     /*

       topic的信息是存储在zk里面了


        数据的压缩是在broker里面进行了压缩，如果consumer来消费的时候，会自动的进行解压缩

      Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 开启 GZIP 压缩
        props.put("compression.type", "gzip");*/

     //   Producer<String, String> producer = new KafkaProducer<>(props);




        Properties props = new Properties();
        props.put("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092");
        //消息的确认机制
        props.put("acks", "all");
        props.put("retries", 0);
        //缓冲区的大小  //默认32M
        props.put("buffer.memory", 33554432);
        //批处理数据的大小，每次写入多少数据到topic   //默认16KB
        props.put("batch.size", 16384);
        //可以延长多久发送数据   //默认为0 表示不等待 ，立即发送
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        //自定义分区的策略
     //   props.put("partitioner.class","cn.kafka.opt.MyPartitioner");
        //指定数据序列化和反序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
        for(int i =0;i<100;i++){
            //既没有指定分区号，也没有数据的key，直接使用轮序的方式将数据发送到各个分区里面去
            //发送的顺序是 helloworld0  ---- helloworld99
            ProducerRecord record = new ProducerRecord("test", "helloworld" + i);
          //  new  ProducerRecord<String,String>()
            new ProducerRecord<String,String>("test","hello","world");
            new ProducerRecord<String,String>("test",2,"hello","world");
         //   new ProducerRecord<String,String>()

            producer.send(record);
        }
        //关闭消息发送客户端
        producer.close();
    }
}