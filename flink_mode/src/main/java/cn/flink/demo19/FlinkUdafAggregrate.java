package cn.flink.demo19;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkUdafAggregrate {
    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.useBlinkPlanner()//Flink1.14开始就删除了其他的执行器了，只保留了BlinkPlanner，默认就是
                //.inStreamingMode()//默认就是StreamingMode
                //.inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        //注册函数
        tableEnvironment.createTemporarySystemFunction("AvgFunc",AvgFunc.class);


        String source_sql = "CREATE TABLE source_score (\n" +
                "  id int,\n" +
                "  name STRING,\n" +
                "  course STRING,\n" +
                "  score Double" +
                ") WITH ( \n " +
                " 'connector' = 'filesystem',\n" +
                " 'path' = 'input/score.csv' , \n" +
                " 'format' = 'csv'\n" +
                ")";

        //创建表
        tableEnvironment.executeSql(source_sql);

        tableEnvironment.from("source_score")
                .groupBy($("course"))
                .select($("course"),call("AvgFunc",$("score").as("avg_score")))
                .execute().print();



        tableEnvironment.executeSql("select course,AvgFunc(score) as avg_score  from source_score group by course")
                .print();

    }

    /**
     * 自定义UDTF函数的话，需要继承AggregateFunction
     */
    public static  class AvgFunc  extends AggregateFunction<Double,AvgAccumulator> {
        @Override
        public Double getValue(AvgAccumulator avgAccumulator) {
            if(avgAccumulator.count==0){
                return null;
            }else {
                return avgAccumulator.sum/avgAccumulator.count;
            }
        }
        //初始化累加器
        @Override
        public AvgAccumulator createAccumulator() {
            return new AvgAccumulator();
        }
        //迭代累加
        public void accumulate(AvgAccumulator acc,Double score){
            acc.setSum(acc.sum+score);
            acc.setCount(acc.count+1);
        }
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static  class AvgAccumulator {

        public double sum = 0.0;
        public int count = 0;

    }
}