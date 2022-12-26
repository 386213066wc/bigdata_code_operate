package cn.func.hive.udaf;


import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class MaxBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {
    // 用于接收结果    private int ans;    public MaxBuffer(){}    public MaxBuffer(int ans){this.ans = ans;}    public int getAns(){        return ans;    }    public void setAns(int ans){        this.ans = ans;    }    public void add(int next){        ans = Math.max(this.ans, next);    }}

    // 用于接收结果
    private int ans;
    public MaxBuffer(){}
    public MaxBuffer(int ans){this.ans = ans;}
    public int getAns(){
        return ans;
    }
    public void setAns(int ans){
        this.ans = ans;
    }
    public void add(int next){
        ans = Math.max(this.ans, next);
    }
}