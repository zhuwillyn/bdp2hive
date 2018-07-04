package com.cecdata.bdp2hive.hive;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.cecdata.bdp2hive.common.EnvInit;
import org.apache.commons.cli.*;

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
 * @description 生成hive分区表组件
 * @mail zhuwillyn@163.com
 * @date 2018/06/29 14:39
 */
public class GenerateHivePartitionTable {

    private void main(String[] args) throws SQLException {
        // 使用Apache common cli封装并解析输入参数
        Options options = new Options();
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("U", "hive_url", true, "The url of Hive connect");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("N", "hive_user", true, "The username of Hive");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("W", "hive_password", false, "The password of Hive");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("u", "mysql_url", true, "The url of MySQL");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("n", "mysql_user", true, "The username of MySQL");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("w", "mysql_password", true, "The password of MySQL");
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
                helpFormatter.printHelp("hive", options, true);
                System.exit(-1);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // 将所有的参数按照短字符名称保存在map里
        Map<String, String> params = new HashMap<String, String>();
        Option[] opts = commandLine.getOptions();
        if (opts != null) {
            for (Option option : opts) {
                String longOpt = option.getLongOpt();
                String shortOpt = option.getOpt();
                String optionValue = commandLine.getOptionValue(shortOpt);
                params.put(shortOpt, optionValue);
            }
        } else {
            System.exit(-1);
        }

        // 根据输入参数获取mysql连接对象
        Connection mysqlConnect = DBUtil.getConn(params.get("u"), params.get("n"), params.get("w"));
        // 根据输入参数获取hive连接对象
        Connection hiveConnect = DBUtil.getHiveConn(params.get("U"), params.get("N"), params.get("W"));
        Statement statement = mysqlConnect.createStatement();
        // 查询数据集下的所有数据集item元素用来作为hive表字段，数据集code作为表名
        ResultSet resultSet = statement.executeQuery("select a.dataset_struc_code,b.dataset_item_code from t_dataset_struc a, t_dataset_item b where a.id=b.fk_dataset_struc_id and a.dataset_struc_level=4");
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        while (resultSet.next()) {
            String key = resultSet.getString(1);
            String value = resultSet.getString(2);
            if (map.containsKey(key)) {
                map.get(key).add(value);
            } else {
                List<String> list = new ArrayList<String>();
                list.add(value);
                map.put(key, list);
            }
        }

        Statement hiveStatement = hiveConnect.createStatement();
        // 遍历数据集集合生成创建分区表SQL语句
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            String tableName = entry.getKey();
            List<String> fields = entry.getValue();
            StringBuffer sb = new StringBuffer("create table if not exists " + tableName + "(");
            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                String temp;
                if (i == (fields.size() - 1)) {
                    temp = field + " string)";
                } else {
                    temp = field + " string,";
                }
                sb.append(temp);
            }
            sb.append(" partitioned by(SJYYLJGDM_PARTITION string)");
            sb.append(" row format delimited fields terminated by '^' stored as textfile");
            System.out.println(sb.toString());
            // 执行SQL语句创建hive分区表
            hiveStatement.execute(sb.toString());
        }

    }

}