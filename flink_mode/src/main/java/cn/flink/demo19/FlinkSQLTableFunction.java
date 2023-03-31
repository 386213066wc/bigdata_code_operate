package cn.flink.demo19;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkSQLTableFunction {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.useBlinkPlanner()//Flink1.14开始就删除了其他的执行器了，只保留了BlinkPlanner，默认就是
                //.inStreamingMode()//默认就是StreamingMode
                .inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        tableEnvironment.createTemporarySystemFunction("JsonFunc",JsonFunction.class);
        tableEnvironment.createTemporarySystemFunction("explodeFunc",ExplodeFunc.class);


        String source_sql = "CREATE TABLE json_table (\n" +
                "  line STRING \n" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='input/product_user.json',\n" +
                "  'format'='raw'\n" +
                ")";

        tableEnvironment.executeSql(source_sql);

        //方式一：使用TableAPI通过内连接来实现
        tableEnvironment.from("json_table")
                .joinLateral(call(ExplodeFunc.class,$("line"),"userBaseList")
                        .as("id","name","begin_time","email"))
                .select(call(JsonFunction.class,$("line"),"date_time"),
                        call(JsonFunction.class,$("line"),"price"),
                        call(JsonFunction.class,$("line"),"productId"),
                        $("id"),
                        $("name"),
                        $("begin_time"),
                        $("email")
                ).execute().print();

        //方式二：使用TableAPI通过左外连接来实现
        tableEnvironment.from("json_table")
                .leftOuterJoinLateral(call(ExplodeFunc.class,$("line"),"userBaseList")
                        .as("id","name","begin_time","email"))
                .select(call(JsonFunction.class,$("line"),"date_time"),
                        call(JsonFunction.class,$("line"),"price"),
                        call(JsonFunction.class,$("line"),"productId"),
                        $("id"),
                        $("name"),
                        $("begin_time"),
                        $("email")
                ).execute().print();







        //方式三：使用FlinkSQL内连接来实现
        tableEnvironment.sqlQuery("select " +
                "JsonFunc(line,'date_time')," +
                "JsonFunc(line,'price')," +
                "JsonFunc(line,'productId')," +
                "id," +
                "name," +
                "begin_time " +
                "email " +
                "  from json_table " +
                ",lateral table(explodeFunc(line,'userBaseList')) "
        ).execute().print();



        //方式四：使用FlinkSQL左外连接来实现
        tableEnvironment.sqlQuery("select " +
                "JsonFunc(line,'date_time') as date_time," +
                "JsonFunc(line,'price') as price ," +
                "JsonFunc(line,'productId') as productId," +
                "id," +
                "name," +
                "begin_time " +
                "email " +
                "  from json_table  left join lateral table (explodeFunc(line,'userBaseList')) as sc(id,name,begin_time,email) on true"
        ).execute().print();

    }


    /**
     * 自定义udf
     */
    public static class JsonFunction extends ScalarFunction {
        public String eval(String line,String key){
            //转换为JSON
            JSONObject baseJson = new JSONObject(line);
            String value = "";
            if(baseJson.has(key)){
                //根据key获取value
                return baseJson.getString(key);
            }
            return value;
        }
    }


    /**
     * 自定义UDTF   一定要继承 TableFunction
     */
    @FunctionHint(output = @DataTypeHint("ROW<id String,name String,begin_time String,email String>"))
    public static class ExplodeFunc  extends TableFunction {

        public void eval(String line,String key){

            JSONObject jsonObject = new JSONObject(line);
            JSONArray jsonArray = new JSONArray(jsonObject.getString(key));
            for(int i = 0;i< jsonArray.length();i++){
                String date_time = jsonArray.getJSONObject(i).getString("begin_time");
                String email = jsonArray.getJSONObject(i).getString("email");
                String id = jsonArray.getJSONObject(i).getString("id");
                String name = jsonArray.getJSONObject(i).getString("name");
                collect(Row.of(id,name,date_time,email));
            }
        }
    }
}