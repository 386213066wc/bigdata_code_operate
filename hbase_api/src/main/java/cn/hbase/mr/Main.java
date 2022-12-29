package cn.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        // 设定绑定的zk集群
        configuration.set("hbase.zookeeper.quorum", "bigdata01:2181,bigdata02:2181,bigdata03:2181");

        int run = ToolRunner.run(configuration, new Main(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(Main.class);

        // mapper
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("myuser"), new Scan(), HBaseReaderMapper.class, Text.class, Put.class, job);
        // reducer
        TableMapReduceUtil.initTableReducerJob("myuser2", HBaseWriterReducer.class, job);

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }
}