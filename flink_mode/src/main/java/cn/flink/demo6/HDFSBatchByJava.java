package cn.flink.demo6;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;

public class HDFSBatchByJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        Path path = new Path(new URI("hdfs://bigdata01:8020/abc.txt"));

        TextInputFormat textInputFormat = new TextInputFormat(path);
        DataSource<String> input = executionEnvironment.createInput(textInputFormat);
        input.print();
    }


}
