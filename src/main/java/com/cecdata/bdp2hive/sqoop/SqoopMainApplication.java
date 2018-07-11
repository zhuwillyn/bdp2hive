package com.cecdata.bdp2hive.sqoop;

import com.cecdata.bdp2hive.common.Constant;
import com.cecdata.bdp2hive.common.DatabaseUtil;
import com.cecdata.bdp2hive.sqoop.util.PropertiesUtil;
import com.cecdata.bdp2hive.sqoop.vo.RefInfo;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author zhuweilin
 * @project transfer-tools
 * @description 根据对照关系生成sqoop脚本将数据导入hdfs中
 * @mail zhuwillyn@163.com
 * @date 2018/05/07 11:19
 */
public class SqoopMainApplication {

    private static Logger logger = LoggerFactory.getLogger(SqoopMainApplication.class);

    static String script = PropertiesUtil.get(Constant.SQOOP.SCRIPT_PARTITION);

    private void main(String[] args) {
        logger.info("resolved arguments");
        // 使用Apache common cli解析输入参数
        Options options = new Options();
        Option opt = new Option(Constant.Cli.Sqoop.HELP_SHORT, Constant.Cli.Sqoop.HELP_LONG, false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(Constant.Cli.Sqoop.SQOOP_BIN_PATH_SHORT, Constant.Cli.Sqoop.SQOOP_BIN_PATH_LONG, true, "The binary path of SQOOP");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(Constant.Cli.Sqoop.MYSQL_URL_SHORT, Constant.Cli.Sqoop.MYSQL_URL_LONG, true, "The url of mysql");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(Constant.Cli.Sqoop.MYSQL_USER_SHORT, Constant.Cli.Sqoop.MYSQL_USER_LONG, true, "The username of database");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(Constant.Cli.Sqoop.MYSQL_PASSWORD_SHORT, Constant.Cli.Sqoop.MYSQL_PASSWORD_LONG, true, "The password for user of database");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(Constant.Cli.Sqoop.ORG_CODE_SHORT, Constant.Cli.Sqoop.ORG_CODE_LONG, true, "The organize code, multi value split by ','");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(Constant.Cli.Sqoop.HIVE_DATABASE_SHORT, Constant.Cli.Sqoop.HIVE_DATABASE_LONG, true, "The database of Hive");
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
                helpFormatter.printHelp("sqoop", options, true);
                System.exit(-1);
            }
        } catch (ParseException e) {
            logger.error(e.getMessage());
            helpFormatter.printHelp("hive", options, true);
            System.exit(-1);
        }

        Map<String, String> arguments = new HashMap<String, String>();
        Option[] opts = commandLine.getOptions();
        if (opts != null) {
            for (Option option : opts) {
                String longOpt = option.getLongOpt();
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
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(new File("./sqoop_scripts_test.sh")));) {
            String orgCodes = params.get(Constant.Cli.Sqoop.ORG_CODE_SHORT);
            String url = params.get(Constant.Cli.Sqoop.MYSQL_URL_SHORT);
            String user = params.get(Constant.Cli.Sqoop.MYSQL_USER_SHORT);
            String password = params.get(Constant.Cli.Sqoop.MYSQL_PASSWORD_SHORT);
            String sqoopBinPath = params.get(Constant.Cli.Sqoop.SQOOP_BIN_PATH_SHORT);
            String hiveDatabase = params.get(Constant.Cli.Sqoop.HIVE_DATABASE_SHORT);
            // 对照关系SQL
            String refSQL = PropertiesUtil.get(Constant.SQOOP.SQOOP_REF_SQL);
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
            ResultSet resultSet = DatabaseUtil.execSQLForResult(url, user, password, refSQL);
            // 处理查询的结果集并按一定规则封装至集合
            Map<String, List<RefInfo>> map = processRefResult(resultSet);
            // 根据数据集code查询数据集下item的SQL语句
            String queryStructItemSQL = PropertiesUtil.get(Constant.SQOOP.SQOOP_ITEM_SQL);
            for (Map.Entry<String, List<RefInfo>> entry : map.entrySet()) {
                // 获取唯一键，分为两部分，机构代码和数据集code，中间使用“|”间隔
                String uniqueId = entry.getKey();
                String[] orgCodeAndStructCode = uniqueId.split("\\|");
                // 格式化查询数据集item的SQL语句，eg:select ... from xxx where code='code'
                String structItemSQL = String.format(queryStructItemSQL, orgCodeAndStructCode[1]);
                // 执行查询语句
                ResultSet sctructItemResult = DatabaseUtil.execSQLForResult(url, user, password, structItemSQL);
                // 处理查询的结果集
                List<String> itemCodes = processStructItemResult(sctructItemResult);
                // 获取对照关系信息，这个集合就是一个机构下的一个数据集的所有对照关系，注：此处为数据集的子集（item的元素可能比基准数据集item数量要少）
                List<RefInfo> refInfos = entry.getValue();
                // 使用map，作用是用来初始让所有数据集item都为空，后面再过滤不为空的数据集，这里是select语句中的片段，eg:"" AS 数据集item_code
                Map<String, String> segment = new LinkedHashMap<String, String>();
                for (String itemCode : itemCodes) {
                    segment.put(itemCode, String.format(" \"\" AS %s,", itemCode));
                }
                // 遍历对照的信息，并覆盖之前的map中的数据集itme，eg: table_name.field_name AS 数据集item_code
                for (RefInfo refInfo : refInfos) {
                    String tableName = refInfo.getTableName();
                    String fieldName = refInfo.getFieldName();
                    String itemCode = refInfo.getItemCode();
                    segment.put(itemCode, String.format(" %s.%s AS %s,", tableName, fieldName, itemCode));
                }
                // 组装SQL语句
                String sql = assembledSQL(segment, refInfos.get(0));
                // 组装sqoop脚本
                String finalScript = assembledScript(refInfos.get(0), sql, sqoopBinPath, hiveDatabase);
                // 将脚本写入shell文件中
                writer.write(finalScript);
                writer.flush();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 组装SQL语句
    private String assembledSQL(Map<String, String> segments, RefInfo refInfo) {
        String orgName = refInfo.getOrgName();
        String orgCode = refInfo.getOrgCode();
        String databaseName = refInfo.getDatabaseName();
        String tableName = refInfo.getTableName();
        // 添加时间戳字段
        segments.put("CUR_TIMESTAMP", " DATE_FORMAT(CURRENT_TIMESTAMP(6),\"%Y%m%d%H%i%S%f\") AS \"CECD_CNVT_TIME\",");
        // 添加四个源信息字段详情
        segments.put(Constant.SQOOP.DATASET_YSJKMC, String.format(" \"%s\" AS %s,", databaseName, Constant.SQOOP.DATASET_YSJKMC));
        segments.put(Constant.SQOOP.DATASET_YSJBMC, String.format(" \"%s\" AS %s,", tableName, Constant.SQOOP.DATASET_YSJBMC));
        segments.put(Constant.SQOOP.DATASET_SJYYLJGMC, String.format(" \"%s\" AS %s,", orgName, Constant.SQOOP.DATASET_SJYYLJGMC));
        segments.put(Constant.SQOOP.DATASET_SJYYLJGDM, String.format(" \"%s\" AS %s,", orgCode, Constant.SQOOP.DATASET_SJYYLJGDM));
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : segments.entrySet()) {
            String sqlSegment = entry.getValue();
            stringBuilder.append(sqlSegment);
        }
        // 去除最后一个逗号
        String sqlSegment = stringBuilder.substring(0, stringBuilder.length() - 1);
        // 拼装成标准的select查询语句
        String sql = String.format("SELECT%s FROM %s", sqlSegment, tableName);
        return sql;
    }

    // 组装脚本，将脚本模板中的参数替换
    private String assembledScript(RefInfo refInfo, String sql, String sqoopBinPath, String hiveDatabase) {
        String databaseAddr = refInfo.getDatabaseAddr();
        String port = refInfo.getPort();
        String databaseName = refInfo.getDatabaseName();
        String orgCode = refInfo.getOrgCode();
        String username = refInfo.getUsername();
        String password = refInfo.getPassword();
        String structCode = refInfo.getStructCode();
        if (StringUtils.isEmpty(hiveDatabase)) {
            hiveDatabase = "default";
        }
        String finalScript = script.replace("${sqoop_path}", sqoopBinPath).replace("${ip}", databaseAddr).replace("${port}", port)
                .replace("${database}", databaseName).replace("${username}", username)
                .replace("${password}", password).replace("${hdfs_path}", structCode)
                .replace("${sql}", sql).replace("${hive_database}", hiveDatabase).replace("${hive_table}", structCode)
                .replace("${partition_value}", orgCode);
        return finalScript;
    }

    // 处理对照关系信息结果集
    private Map<String, List<RefInfo>> processRefResult(ResultSet resultSet) {
        Map<String, List<RefInfo>> maps = new HashMap<String, List<RefInfo>>();
        if (resultSet != null) {
            try {
                while (resultSet.next()) {
                    String orgName = resultSet.getString(Constant.SQOOP.COLUMN_ORG_NAME);
                    String orgCode = resultSet.getString(Constant.SQOOP.COLUMN_ORG_CODE);
                    String databaseName = resultSet.getString(Constant.SQOOP.COLUMN_DATABASE_NAME);
                    String tableName = resultSet.getString(Constant.SQOOP.COLUMN_TABLE_NAME);
                    String fieldName = resultSet.getString(Constant.SQOOP.COLUMN_FIELD_NAME);
                    String itemCode = resultSet.getString(Constant.SQOOP.COLUMN_ITEM_CODE);
                    String structCode = resultSet.getString(Constant.SQOOP.COLUMN_STRUCT_CODE);
                    String databaseAddr = resultSet.getString(Constant.SQOOP.COLUMN_DATABASE_ADDRESS);
                    String port = resultSet.getString(Constant.SQOOP.COLUMN_ACCESS_PORT);
                    String username = resultSet.getString(Constant.SQOOP.COLUMN_USERNAME);
                    String password = resultSet.getString(Constant.SQOOP.COLUMN_PASSWORD);
                    RefInfo refInfo = new RefInfo(orgName, orgCode, databaseName, tableName, fieldName, itemCode, structCode, databaseAddr, port, username, password);
                    // 为了防止重复，使用机构代码和数据集code组成唯一的一串码，能够定位到机构数据集（最小单位）
                    String uniqueId = String.format("%s|%s", orgCode, structCode);
                    if (maps.containsKey(uniqueId)) {
                        maps.get(uniqueId).add(refInfo);
                    } else {
                        List<RefInfo> refInfos = new ArrayList<RefInfo>();
                        refInfos.add(refInfo);
                        maps.put(uniqueId, refInfos);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return maps;
    }

    // 处理数据集item结果集
    private List<String> processStructItemResult(ResultSet resultSet) {
        List<String> itemCodes = new LinkedList<String>();
        if (resultSet != null) {
            try {
                while (resultSet.next()) {
                    String structItemCode = resultSet.getString(Constant.SQOOP.COLUMN_STRUCT_ITEM_CODE);
                    itemCodes.add(structItemCode);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return itemCodes;
    }

}
