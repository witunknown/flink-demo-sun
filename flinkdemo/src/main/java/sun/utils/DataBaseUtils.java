package sun.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Sun
 * @version 1.0.0
 * @ClassName DataBaseUtil.java
 * @Description TODO
 * @createTime 2021年04月08日 22:26:00
 */
public class DataBaseUtils {

    private static ReentrantLock lock = new ReentrantLock();
    private static volatile Connection connection = getConnection();

    private static void init() {
        try {
            Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("clickhouse类未找到", e);
        }
        try {
            connection = DriverManager.getConnection("jdbc:clickhouse://192.168.171.128:9000");
        } catch (SQLException e) {
            throw new RuntimeException("连接失败", e);
        }

    }


    public static Connection getConnection() {
        if (connection == null) {
            try {
                lock.lock();
                if (connection == null) {
                    init();
                }
            } finally {
                lock.unlock();
            }
        }
        return connection;
    }
}
