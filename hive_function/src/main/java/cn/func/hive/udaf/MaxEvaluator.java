package cn.func.hive.udaf;


import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class MaxEvaluator extends GenericUDAFEvaluator {

    private PrimitiveObjectInspector in;  // 输入的参数
    private ObjectInspector out;  // 输出的参数
    private PrimitiveObjectInspector buffer;  // 缓冲区

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new MaxBuffer();
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        super.init(m,parameters);
        if (Mode.PARTIAL1.equals(m) || Mode.COMPLETE.equals(m) ){
            in = (PrimitiveObjectInspector) parameters[0];
        }else{
            buffer = (PrimitiveObjectInspector) parameters[0];
        }

        // 输出的类型
        out = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        return out;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        ((MaxBuffer)agg).setAns(0);
    }

    @Override
    // 每行执行一次
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        // 调用缓冲区数据
        ((MaxBuffer)agg).add((Integer) parameters[0]);
    }

    @Override
    // 预聚合
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {

        return terminate(agg);
    }

    @Override
    // 合并缓冲区
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        int in = (int) buffer.getPrimitiveJavaObject(partial);
        int ans = ((MaxBuffer) agg).getAns();
        ((MaxBuffer)agg).add(in);
    }

    @Override
    // 最终聚合
    public Object terminate(AggregationBuffer agg) throws HiveException {
        return ((MaxBuffer)agg).getAns();
    }
}
