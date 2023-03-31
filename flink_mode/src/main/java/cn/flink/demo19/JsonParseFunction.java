package cn.flink.demo19;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

//自定义函数，必须要继承ScalarFunction
public class JsonParseFunction extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {

    }


    /**
     * 覆写eval这个方法来实现自定义函数的逻辑
     * @param jsonLine
     * @param key
     * @return
     */
    public String eval(String jsonLine,String key){
        JSONObject jsonObject = JSONObject.parseObject(jsonLine);
        if(jsonObject.containsKey(key)){
            return   jsonObject.getString(key);
        }else{
            return "";
        }

    }



    @Override
    public void close() throws Exception {

    }
}