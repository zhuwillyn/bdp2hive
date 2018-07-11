package com.cecdata.bdp2hive.common;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author zhuweilin
 * @project bdp2hive
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/07/10 18:08
 */
public class DatabaseUtil {

    private static DruidDataSource dataSource;
    private static DruidDataSource hiveDatasource;
    private static Statement statement;
    private static Statement hiveStatement;

    private static void initDatasource(Properties props) {
        try {
            dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initHiveDatasource(Properties props) {
        try {
            hiveDatasource = (DruidDataSource) DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DruidPooledConnection getConn(String url, String username, String password) {
        DruidPooledConnection connection = null;
        if (dataSource == null) {
            Properties props = new Properties();
            props.setProperty("url", url);
            props.setProperty("username", username);
            props.setProperty("password", password);
            initDatasource(props);
        }
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static DruidPooledConnection getHiveConn(String url, String username, String password) {
        DruidPooledConnection connection = null;
        if (dataSource == null) {
            Properties props = new Properties();
            props.setProperty("url", url);
            props.setProperty("username", username);
            props.setProperty("password", password);
            initHiveDatasource(props);
        }
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static Statement getStmt(String url, String username, String password) {
        if (statement == null) {
            DruidPooledConnection connection = getConn(url, username, password);
            try {
                statement = connection.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return statement;
    }

    public static Statement getHiveStmt(String url, String username, String password) {
        if (hiveStatement == null) {
            DruidPooledConnection connection = getHiveConn(url, username, password);
            try {
                statement = connection.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return hiveStatement;
    }

    public static boolean execSQL(String url, String username, String password, String sql) {
        boolean flag = false;
        Statement statement = getStmt(url, username, password);
        try {
            flag = statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return flag;
    }

    public static ResultSet execSQLForResult(String url, String username, String password, String sql) {
        ResultSet resultSet = null;
        Statement statement = getStmt(url, username, password);
        try {
            resultSet = statement.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }


    public static boolean execHiveSQL(String url, String username, String password, String sql) {
        boolean flag = false;
        Statement statement = getHiveStmt(url, username, password);
        try {
            flag = statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return flag;
    }

    public static ResultSet execHiveSQLForResult(String url, String username, String password, String sql) {
        ResultSet resultSet = null;
        Statement statement = getHiveStmt(url, username, password);
        try {
            resultSet = statement.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

}
