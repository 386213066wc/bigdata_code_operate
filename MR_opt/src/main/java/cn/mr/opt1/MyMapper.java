package cn.mr.opt1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    /**
     *
     * @param key  输入的行偏移量 ，没啥用
     * @param value  一行文本文件 需要将文本文件给切开
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Text text = new Text();
        IntWritable intWritable = new IntWritable(1);
        String[] split = value.toString().split(",");
        for (String s : split) {
            text.set(s);
            context.write(text,intWritable);
        }
    }
}
