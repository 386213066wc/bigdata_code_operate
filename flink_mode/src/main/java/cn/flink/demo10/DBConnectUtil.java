package cn.flink.demo10;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * todo: 数据库连接工具类
 */
public class DBConnectUtil {
    private static final Logger log = LoggerFactory.getLogger(DBConnectUtil.class);

    /**
     * todo: 获取连接
     *
     * @param url
     * @param user
     * @param password
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String url, String user, String password) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("获取mysql.jdbc.Driver失败");
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(url, user, password);
            log.info("获取连接:{} 成功...",conn);
        }catch (Exception e){
            log.error("获取连接失败，url:" + url + ",user:" + user);
        }

        //设置手动提交
        conn.setAutoCommit(false);
        return conn;

    }

    /**
     * todo: 提交事务
     */
    public static void commit(Connection conn) {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                log.error("提交事务失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * todo:事务回滚
     *
     * @param conn
     */
    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                log.error("事务回滚失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * todo:关闭连接
     * @param conn
     */
    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("关闭连接失败,Connection:" + conn);
                e.printStackTrace();
            }
        }
    }
}
