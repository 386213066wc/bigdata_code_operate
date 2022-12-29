package cn.hbase.bulkload;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        //构建rowkey
        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(split[0].getBytes(StandardCharsets.UTF_8));
        //构建put对象
        Put put = new Put(split[0].getBytes(StandardCharsets.UTF_8));
        put.addColumn("f1".getBytes(),"name".getBytes(),split[1].getBytes());
        put.addColumn("f1".getBytes(),"age".getBytes(),split[2].getBytes());

        context.write(rowkey,put);


    }
}
