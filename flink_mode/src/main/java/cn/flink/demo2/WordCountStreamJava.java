package cn.flink.demo2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamJava {

    public static void main(String[] args) throws Exception {
        //从socket里面去读取数据  统计每个单词出现的次数
        //流式程序处理入口类  StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置执行环境级别的并行度
        env.setParallelism(4);

        DataStreamSource<String> sourceDStream = env.socketTextStream("bigdata01", 9999);

        SingleOutputStreamOperator<WordCount> wordAndOneStream = sourceDStream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String line, Collector<WordCount> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new WordCount(word, 1L));
                }


            }
        })/*.setParallelism(4)*/;

        SingleOutputStreamOperator<WordCount> resultSum = wordAndOneStream.keyBy("word")
                .sum("count")/*.setParallelism(8)*/;

        resultSum.print();
        env.execute();


    }



    public static class WordCount{

        public  WordCount(){}

        public WordCount(String word,long count){
            this.word = word;
            this.count = count;
        }


        public String word;
        public long count;

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }



}
