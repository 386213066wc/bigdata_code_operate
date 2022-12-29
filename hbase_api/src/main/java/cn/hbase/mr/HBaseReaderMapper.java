package cn.hbase.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//集成mapreduce可以通过mr来计算hbase当中的数据
public class HBaseReaderMapper  extends TableMapper<Text, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Put>.Context context) throws IOException, InterruptedException {

        byte[] rowkeyBytes = key.get();
        String rowkeyStr = Bytes.toString(rowkeyBytes);
        Text text = new Text(rowkeyStr);

        Put put = new Put(rowkeyBytes);
        Cell[] cells = value.rawCells();
        for (Cell cell : cells) {
            byte[] familyBytes = CellUtil.cloneFamily(cell);
            String familyStr = Bytes.toString(familyBytes);
            if("f1".equals(familyStr)){
                byte[] qualifier_bytes = CellUtil.cloneQualifier(cell);
                String qualifierStr = Bytes.toString(qualifier_bytes);
                if("name".equals(qualifierStr)){
                    put.add(cell);
                }
                if("age".equals(qualifierStr)){
                    put.add(cell);
                }



            }

        }

        if(!put.isEmpty()){
            context.write(text,put);
        }



    }
}
