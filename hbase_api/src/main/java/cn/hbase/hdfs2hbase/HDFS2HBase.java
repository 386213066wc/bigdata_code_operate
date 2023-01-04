package cn.hbase.hdfs2hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 将HDFS上文件/hbase/input/user.txt数据，导入到HBase的myuser2表
 */
public class HDFS2HBase {
    public static class HdfsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        // 数据原样输出
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class HBaseReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {

        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            /**
             * key -> 一行数据
             * 样例数据：
             * 0007	zhangsan	18
             * 0008	lisi	25
             * 0009	wangwu	20
             */
            String[] split = key.toString().split("\t");

            Put put = new Put(Bytes.toBytes(split[0]));
            put.addColumn("f1".getBytes(), "name".getBytes(), split[1].getBytes());
            put.addColumn("f1".getBytes(), "age".getBytes(), split[2].getBytes());

            context.write(new ImmutableBytesWritable(Bytes.toBytes(split[0])), put);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        // 设定zk集群
        conf.set("hbase.zookeeper.quorum", "bigdata01:2181,bigdata02:2181,bigdata03:2181");
        Job job = Job.getInstance(conf);

        job.setJarByClass(HDFS2HBase.class);

        // 可省略
        // job.setInputFormatClass(TextInputFormat.class);
        // 输入文件路径
        FileInputFormat.addInputPath(job, new Path("hdfs://bigdata01:8020/hbase/input"));

        job.setMapperClass(HdfsMapper.class);
        // map端的输出的key value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 指定输出到hbase的表名
        TableMapReduceUtil.initTableReducerJob("myuser2", HBaseReducer.class, job);

        // 设置reduce个数
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
