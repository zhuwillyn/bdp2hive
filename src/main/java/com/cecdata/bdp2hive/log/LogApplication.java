package com.cecdata.bdp2hive.log;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.cecdata.bdp2hive.common.Constant;
import com.cecdata.bdp2hive.common.DatabaseUtil;
import com.cecdata.bdp2hive.sqoop.util.PropertiesUtil;
import com.cecdata.bdp2hive.sqoop.vo.RefInfo;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhuweilin
 * @project bdp2hive
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/07/12 10:37
 */
public class LogApplication {

    private Logger logger = LoggerFactory.getLogger(LogApplication.class);

    private void main(String[] args) {
        logger.info("resolved arguments");
        // 使用Apache common cli解析输入参数
        Options options = new Options();
        Option opt = new Option(Constant.Cli.Log.HELP_SHORT, Constant.Cli.Log.HELP_LONG, false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.MYSQL_URL_SHORT, Constant.Cli.Log.MYSQL_URL_LONG, true, "The url of MySQL");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.MYSQL_USER_SHORT, Constant.Cli.Log.MYSQL_USER_LONG, true, "The user of mysql");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.MYSQL_PASSWORD_SHORT, Constant.Cli.Log.MYSQL_PASSWORD_LONG, true, "The password for user of database");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.HIVE_URL_SHORT, Constant.Cli.Log.HIVE_URL_LONG, true, "The url of Hive");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.HIVE_USER_SHORT, Constant.Cli.Log.HIVE_USER_LONG, true, "The user of Hive");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.HIVE_PASSWORD_SHORT, Constant.Cli.Log.HIVE_PASSWORD_LONG, true, "The password for user of Hive");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.POSTGRESSQL_USER_SHORT, Constant.Cli.Log.POSTGRESSQL_USER_LONG, true, "The user of PostgreSQL");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.POSTGRESSQL_URL_SHORT, Constant.Cli.Log.POSTGRESSQL_URL_LONG, true, "The url of PostgreSQL");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.POSTGRESSQL_PASSWORD_SHORT, Constant.Cli.Log.POSTGRESSQL_PASSWORD_LONG, true, "The database of PostgreSQL");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(Constant.Cli.Log.ORG_CODE_SHORT, Constant.Cli.Log.ORG_CODE_LONG, true, "The organize code, multi value split by ','");
        opt.setRequired(false);
        options.addOption(opt);

        CommandLineParser parser = new PosixParser();
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(120);
        CommandLine commandLine = null;
        try {
            // 当输入参数为空或者"-h" "--help"时打印帮助信息
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption("h") || commandLine.getOptions().length == 0) {
                logger.info("for more information please use \"-h\" or \"--help\"");
                helpFormatter.printHelp("log", options, true);
                System.exit(-1);
            }
        } catch (ParseException e) {
            logger.error(e.getMessage());
            helpFormatter.printHelp("log", options, true);
            System.exit(-1);
        }

        Map<String, String> arguments = new HashMap<String, String>();
        Option[] opts = commandLine.getOptions();
        if (opts != null) {
            for (Option option : opts) {
                // String longOpt = option.getLongOpt();
                String shortOpt = option.getOpt();
                String optionValue = commandLine.getOptionValue(shortOpt);
                arguments.put(shortOpt, optionValue);
            }
        } else {
            logger.error("the opts was null");
            System.exit(-1);
        }
        start(arguments);
    }

    private void start(Map<String, String> params) {
        Connection hiveConn = null;
        Connection pgConn = null;
        Connection connection = null;
        Statement statement = null;
        Statement hiveStmt = null;
        try {
            String orgCodes = params.get(Constant.Cli.Log.ORG_CODE_SHORT);
            String url = params.get(Constant.Cli.Log.MYSQL_URL_SHORT);
            String user = params.get(Constant.Cli.Log.MYSQL_USER_SHORT);
            String password = params.get(Constant.Cli.Log.MYSQL_PASSWORD_SHORT);
            String hiveUrl = params.get(Constant.Cli.Log.HIVE_URL_SHORT);
            String hiveUser = params.get(Constant.Cli.Log.HIVE_USER_SHORT);
            String hivePassword = params.get(Constant.Cli.Log.HIVE_PASSWORD_SHORT);
            String pgUrl = params.get(Constant.Cli.Log.POSTGRESSQL_URL_SHORT);
            String pgUser = params.get(Constant.Cli.Log.POSTGRESSQL_USER_SHORT);
            String pgPassword = params.get(Constant.Cli.Log.POSTGRESSQL_PASSWORD_SHORT);
            // 对照关系SQL
            String refSQL = PropertiesUtil.get(Constant.SQOOP.LOG_REF_SQL);
            // SQL条件
            String refSQLCondition = PropertiesUtil.get(Constant.SQOOP.SQOOP_REF_SQL_CONDITION);
            // 判断code是否为空，为空则查询所有机构下的对照关系
            if (StringUtils.isNotEmpty(orgCodes)) {
                String[] arrCodes = orgCodes.split(",");
                String codes = "";
                // 将code 之间用逗号间隔，eg:'code1','code2'
                for (String code : arrCodes) {
                    codes += String.format("'%s',", code);
                }
                // 删除最后一个逗号
                codes = codes.substring(0, codes.length() - 1);
                // 使用模板替换拼接好的code，eg: AND field_name in (%s) "'code1','code2'" ===> field_name in ('code1', 'code2')
                refSQLCondition = String.format(" " + refSQLCondition, codes);
                // 拼装SQL，eg:select ... from ... where field in ('code1','code2'...)
                refSQL = refSQL + refSQLCondition;
            }
            // 根据最终的SQL查询数据集与字段对照关系
            connection = DatabaseUtil.getConn(url, user, password);
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(refSQL);
            // 处理查询的结果集并按一定规则封装至集合
            List<RefInfo> refInfos = processRefResult(resultSet);

            hiveConn = DatabaseUtil.getHiveConn(hiveUrl, hiveUser, hivePassword);
            hiveStmt = hiveConn.createStatement();
            pgConn = DatabaseUtil.getPgConn(pgUrl, pgUser, pgPassword);
            String srcCountSQL = PropertiesUtil.get(Constant.SQOOP.LOG_COUNT_SQL);
            List<Log> logList = new ArrayList<Log>();
            for (RefInfo refInfo : refInfos) {
                String databaseAddr = refInfo.getDatabaseAddr();
                String port = refInfo.getPort();
                String databaseName = refInfo.getDatabaseName();
                String username = refInfo.getUsername();
                String _password = refInfo.getPassword();
                String tableName = refInfo.getTableName();
                String structCode = refInfo.getStructCode();
                String orgCode = refInfo.getOrgCode();

                String srcUrl = String.format("jdbc:mysql://%s:%s/%s", databaseAddr, port, databaseName);
                Connection srcConn = DatabaseUtil.getConn(srcUrl, username, _password);
                Statement srcStmt = srcConn.createStatement();
                ResultSet srcResultSet = srcStmt.executeQuery(String.format(srcCountSQL, tableName));
                long srcCount = processCountResultSet(srcResultSet);
                ResultSet hiveResultSet = hiveStmt.executeQuery(String.format(srcCountSQL + " where SJYYLJGDM_PARTITION='%s'", structCode, orgCode));
                long hiveCount = processCountResultSet(hiveResultSet);
                // 对比状态
                int status = (srcCount == hiveCount) ? 0 : 1;
                Log log = new Log();
                log.setSrcOrgName(refInfo.getOrgName());
                log.setSrcOrgCode(orgCode);
                log.setSrcUrl(srcUrl);
                log.setSrcDatabase(databaseName);
                log.setSrcTable(tableName);
                log.setSrcCount(srcCount);
                log.setDstUrl(hiveUrl);
                log.setDstDatabase("");
                log.setDstTable(structCode);
                log.setDstCount(hiveCount);
                log.setStatus(status);
                logList.add(log);
                srcStmt.closeOnCompletion();
                DatabaseUtil.recycleConn(srcConn);
            }
            processInsertLog(logList, pgConn);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if(hiveStmt != null){
                try {
                    hiveStmt.closeOnCompletion();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statement != null){
                try {
                    statement.closeOnCompletion();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            DatabaseUtil.recycleHiveConn(hiveConn);
            DatabaseUtil.recycleHiveConn(pgConn);
            DatabaseUtil.recycleConn(connection);
        }

    }

    // 批量插入log方法
    private void processInsertLog(List<Log> logList, Connection conn) throws SQLException {
        // 创建log表SQL语句
        String createSQL = PropertiesUtil.get(Constant.SQOOP.LOG_CREATE_SQL);
        // 插入log表基础模板SQL语句
        String insertSQL = PropertiesUtil.get(Constant.SQOOP.LOG_INSERT_SQL);
        Statement statement = conn.createStatement();
        // 创建log表操作
        statement.executeUpdate(createSQL);
        int size = logList.size();
        if (size > 0) {
            // 循环遍历批量插入
            for (int i = 0; i < size; i++) {
                Log log = logList.get(i);
                String sql = String.format(insertSQL, log.getSrcOrgName(), log.getSrcOrgCode(),
                        log.getSrcUrl(), log.getSrcDatabase(), log.getSrcTable(),
                        log.getSrcCount(),log.getDstUrl(),log.getDstDatabase(),
                        log.getDstTable(),log.getDstCount(),log.getStatus());
                statement.addBatch(sql);
                // 每一百条执行一次批量插入
                if (i % 100 == 0){
                    statement.executeBatch();
                }
            }
            // 最后未满100条数据批量插入
            statement.executeBatch();
        }
        // 关闭资源
        statement.closeOnCompletion();
    }

    private List<RefInfo> processRefResult(ResultSet resultSet) {
        List<RefInfo> refInfoList = new ArrayList<RefInfo>();
        if (resultSet != null) {
            try {
                while (resultSet.next()) {
                    String orgName = resultSet.getString(Constant.SQOOP.COLUMN_ORG_NAME);
                    String orgCode = resultSet.getString(Constant.SQOOP.COLUMN_ORG_CODE);
                    String databaseName = resultSet.getString(Constant.SQOOP.COLUMN_DATABASE_NAME);
                    String tableName = resultSet.getString(Constant.SQOOP.COLUMN_TABLE_NAME);
                    String structCode = resultSet.getString(Constant.SQOOP.COLUMN_STRUCT_CODE);
                    String databaseAddr = resultSet.getString(Constant.SQOOP.COLUMN_DATABASE_ADDRESS);
                    String port = resultSet.getString(Constant.SQOOP.COLUMN_ACCESS_PORT);
                    String username = resultSet.getString(Constant.SQOOP.COLUMN_USERNAME);
                    String password = resultSet.getString(Constant.SQOOP.COLUMN_PASSWORD);
                    RefInfo refInfo = new RefInfo(orgName, orgCode, databaseName, tableName, structCode, databaseAddr, port, username, password);
                    refInfoList.add(refInfo);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return refInfoList;
    }

    private long processCountResultSet(ResultSet resultSet) {
        long count = 0L;
        if (resultSet != null) {
            try {
                count = resultSet.getLong(1);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return count;
    }

}
