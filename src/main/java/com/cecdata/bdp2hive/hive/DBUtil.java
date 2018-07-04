package com.cecdata.bdp2hive.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author zhuweilin
 * @project spark-poc
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/06/20 10:49
 */
public class DBUtil {

    public static Connection connection;
    public static Statement statement;
    private static String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private DBUtil() {
    }

    public static Connection getConn(String url, String user, String password) {
        try {
            Class.forName(MYSQL_DRIVER);
            connection = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static Connection getHiveConn(String url, String user, String password) {
        try {
            Class.forName(HIVE_DRIVER);
            connection = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static Statement getStmt() {
        try {
            if (statement == null)
                statement = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return statement;
    }

}
