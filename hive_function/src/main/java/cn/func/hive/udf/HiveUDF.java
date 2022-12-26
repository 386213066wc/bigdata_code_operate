package cn.func.hive.udf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义udf函数
 * 实现计算给定字符串的长度
 * abcd   ===> UDF  ==>  4
 */
public class HiveUDF extends GenericUDF {

    /***
     * 初始化的方法
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 1 ){
            throw  new UDFArgumentException("输入参数异常，只能输入一个参数");
        }
        //只允许输入的参数的类型是字符串，不让输入map，struct，list等等
        if(!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw  new UDFArgumentTypeException(0,"输入参数类型异常");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        //给所有传入的字符串，加一个后缀.com
    //    String newStr = arguments[0].get().toString() + ".com";
        if(arguments[0].get() == null){
            return  0;
        }else{
            int length = arguments[0].get().toString().length();
            return length;

        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
