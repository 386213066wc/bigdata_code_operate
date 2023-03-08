package cn.flink.demo6;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;

public class HDFStreamByJava {
    public static void main(String[] args) throws Exception {
        //获取流式处理程序的入口类
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();



        Path path = new Path("hdfs://bigdata01:8020/abc.txt");

        HadoopInputFormat<LongWritable, Text> inputFormat = HadoopInputs.readHadoopFile(new TextInputFormat(), LongWritable.class, Text.class, "hdfs://bigdata01:8020/abc.txt");
        //读取数据
        DataStreamSource<Tuple2<LongWritable, Text>> input = executionEnvironment.createInput(inputFormat);
        input.print();

        //写出数据到hdfs上面去
        SingleOutputStreamOperator<String> outResult = input.map(new MapFunction<Tuple2<LongWritable, Text>, String>() {
            @Override
            public String map(Tuple2<LongWritable, Text> value) throws Exception {
                return value.f1.toString();
            }
        });
        Path path2 = new Path("hdfs://bigdata01:8020/path_out");
        //使用了构造者模式来创建FileSink这个对象
        FileSink.DefaultRowFormatBuilder<String> stringDefaultRowFormatBuilder = FileSink.forRowFormat(path2, new SimpleStringEncoder<String>());
        FileSink<String> fileSink = stringDefaultRowFormatBuilder.build();


        //使用outputStream将数据流给输出到某个地方去

        outResult.sinkTo(fileSink).setParallelism(1);
        executionEnvironment.execute();
    }
}