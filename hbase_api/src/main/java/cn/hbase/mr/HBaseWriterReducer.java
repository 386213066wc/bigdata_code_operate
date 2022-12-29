package cn.hbase.mr;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HBaseWriterReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<Put> values, Reducer<Text, Put, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
        immutableBytesWritable.set(key.toString().getBytes(StandardCharsets.UTF_8));

        for (Put value : values) {
            context.write(immutableBytesWritable,value);
        }

    }
}
