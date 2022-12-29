package cn.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;

import java.io.IOException;

public class LoadData {

    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","bigdata01,bigdata02,bigdata03");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("myuser2");
        Table table = connection.getTable(TableName.valueOf("myuser2"));
        BulkLoadHFiles load = BulkLoadHFiles.create(configuration);
        load.bulkLoad(tableName,new Path("hdfs://bigdata01:8020/hbase/out_hfile"));
    }


}
