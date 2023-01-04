package com.opt.flume.sink;


import org.apache.flume.conf.Configurable;
import org.apache.flume.*;
import org.apache.flume.sink.AbstractSink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 自定义MysqlSink
 */
public class MysqlSink extends AbstractSink implements Configurable {
    private String mysqlurl = "";
    private String username = "";
    private String password = "";
    private String tableName = "";

    Connection con = null;

    @Override
    public Status process() {
        Status status = null;
        // Start transaction 获得Channel对象
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();

        try {
            Event event = ch.take();

            if (event != null) {
                //获取body中的数据
                String body = new String(event.getBody(), "UTF-8");

                //如果日志中有以下关键字的不需要保存，过滤掉
                if (body.contains("delete") || body.contains("drop") || body.contains("alert")) {
                    status = Status.BACKOFF;
                } else {

                    //存入Mysql
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String createtime = df.format(new Date());

                    PreparedStatement stmt = con.prepareStatement("insert into " + tableName + " (createtime, content) values (?, ?)");
                    stmt.setString(1, createtime);
                    stmt.setString(2, body);
                    stmt.execute();
                    stmt.close();
                    status = Status.READY;
                }
            } else {
                status = Status.BACKOFF;
            }

            txn.commit();
        } catch (Throwable t) {
            txn.rollback();
            t.getCause().printStackTrace();
            status = Status.BACKOFF;
        } finally {
            txn.close();
        }

        return status;
    }

    /**
     * 获取配置文件中指定名称的参数值
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        /*
            getString的参数与agent配置文件中sink组件的自定义的属性名一一对应
            如context.getString("mysqlurl");对应
            a1.sinks.k1.mysqlurl=jdbc:mysql://bigdata03:3306/mysqlsource?useSSL=false
         */
        mysqlurl = context.getString("mysqlurl");
        username = context.getString("username");
        password = context.getString("password");
        tableName = context.getString("tablename");
    }

    @Override
    public synchronized void start() {
        try {
            //初始化数据库连接
            con = DriverManager.getConnection(mysqlurl, username, password);
            super.start();
            System.out.println("finish start");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public synchronized void stop() {
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        super.stop();
    }

}