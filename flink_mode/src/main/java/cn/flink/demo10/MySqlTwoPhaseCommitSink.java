package cn.flink.demo10;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * todo: 自定义kafka to mysql，继承TwoPhaseCommitSinkFunction,实现两阶段提交。
 * 功能：保证kafak to mysql 的Exactly-Once
 */

public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<ObjectNode,MySqlTwoPhaseCommitSink.ConnectionState,Void> {

    //todo: 提供构造方法
    public MySqlTwoPhaseCommitSink(){
        super(new KryoSerializer(MySqlTwoPhaseCommitSink.ConnectionState.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * todo: 执行数据库入库操作  task初始化的时候调用  发送的数据格式为 数字
     * @param connectionState
     * @param objectNode
     * @param context
     * @throws Exception
     */
    protected void invoke(MySqlTwoPhaseCommitSink.ConnectionState connectionState, ObjectNode objectNode, Context context) throws Exception {
        System.out.println("start invoke...");
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String value = objectNode.get("value").toString();
        System.out.println("value:"+value );

        String sql = "insert into `test` (`value`,`insert_time`) values (?,?)";
        PreparedStatement ps = connectionState.connection.prepareStatement(sql);
        Timestamp insert_time = new Timestamp(System.currentTimeMillis());
        ps.setString(1, value);
        ps.setTimestamp(2, insert_time);

        System.out.println("要插入的数据:" + value +" ---> "+insert_time);

        //执行insert语句
        ps.execute();

        //手动制造异常
        if(Integer.parseInt(value) == 5) {
            System.out.println(1 / 0);
        }
    }

    /**
     * todo: 获取连接，开启手动提交事物（getConnection方法中）
     * @return
     * @throws Exception
     */
    protected MySqlTwoPhaseCommitSink.ConnectionState beginTransaction() throws Exception {
        //log.info("start beginTransaction.......");
        String url = "jdbc:mysql://bigdata03:3306/testDB?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
        Connection connection = DBConnectUtil.getConnection(url,"root","123456");
        return new ConnectionState(connection);
    }

    /**
     *todo: 预提交，这里预提交的逻辑在invoke方法中
     * @param connectionState
     * @throws Exception
     */
    protected void preCommit(MySqlTwoPhaseCommitSink.ConnectionState connectionState) throws Exception {
        //log.info("start preCommit...");
    }


    /**
     * todo: 如果invoke方法执行正常，则提交事务
     * @param connectionState
     */
    protected void commit(MySqlTwoPhaseCommitSink.ConnectionState connectionState) {
        //log.info("start commit...");
        DBConnectUtil.commit(connectionState.connection);
    }

    /**
     * todo: 如果invoke执行异常则abort回滚事物，下一次的checkpoint操作也不会执行
     * @param connectionState
     */
    protected void abort(MySqlTwoPhaseCommitSink.ConnectionState connectionState) {
       // log.info("start abort rollback...");
        DBConnectUtil.rollback(connectionState.connection);
    }


    //封装Connection连接类
    static class ConnectionState {

        private final transient Connection connection;

        ConnectionState(Connection connection) {
            this.connection = connection;
        }

    }
}