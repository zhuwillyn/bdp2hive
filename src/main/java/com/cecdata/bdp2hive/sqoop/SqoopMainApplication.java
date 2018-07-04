package com.cecdata.bdp2hive.sqoop;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cecdata.bdp2hive.common.Constant;
import com.cecdata.bdp2hive.common.EnvInit;
import com.cecdata.bdp2hive.common.mapper.Mapper;
import com.cecdata.bdp2hive.common.vo.DB;
import com.cecdata.bdp2hive.common.vo.Struct;
import com.cecdata.bdp2hive.sqoop.service.ParseExcel;
import com.cecdata.bdp2hive.sqoop.util.OkHttpUtil;
import com.cecdata.bdp2hive.sqoop.util.PropertiesUtil;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author zhuweilin
 * @project transfer-tools
 * @description 根据对照关系生成sqoop脚本将数据导入hdfs中
 * @mail zhuwillyn@163.com
 * @date 2018/05/07 11:19
 */
public class SqoopMainApplication {

    private static Mapper mapper;
    String refUrl = PropertiesUtil.get(Constant.SQOOP.REF_URL);
    String sqlUrl = PropertiesUtil.get(Constant.SQOOP.SQL_URL);
    String sqlParam = PropertiesUtil.get(Constant.SQOOP.SQL_PARAM);
    String script = PropertiesUtil.get(Constant.SQOOP.SCRIPT_PARTITION);

    private void main(String[] args) {
        // 使用Apache common cli解析输入参数
        Options options = new Options();
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "file_path", true, "The path of the excel file");
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
                helpFormatter.printHelp("sqoop", options, true);
                System.exit(-1);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        // 直接获取参数值
        String filePath = commandLine.getOptionValue("file_path");
        ApplicationContext context = EnvInit.getContext();
        mapper = context.getBean(Mapper.class);
        if (StringUtils.isNotEmpty(filePath))
            start(filePath);
    }

    private void start(String filePath) {
        File file = new File(filePath);
        // String absolutePath = file.getAbsolutePath();
        // 解析数据库表字段对照关系excel文件,获取库表字段层级关系
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(new File("./sqoop_scripts_test.sh")));) {
            Map<String, Set<String>> listMap = new HashMap<String, Set<String>>();
            if (file.isDirectory()) {
                // 如果传入的是目录，则比那里目录下的所有xlsx文件
                File[] files = file.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File pathname) {
                        String name = pathname.getName();
                        if (name.endsWith(".xlsx")) {
                            return true;
                        }
                        return false;
                    }
                });
                for (File _file : files) {
                    System.out.println("解析文件：" + _file.getName());
                    Map<String, Set<String>> parse = ParseExcel.parse(_file);
                    listMap.putAll(parse);
                }
            } else {
                listMap.putAll(ParseExcel.parse(file));
            }
            // map中存储：key:数据库名,value:[数据集1，数据集2]
            for (Map.Entry<String, Set<String>> entry : listMap.entrySet()) {
                String connName = entry.getKey();
                if (StringUtils.isNotEmpty(connName)) {
                    // 根据数据库连接名查找数据库信息、机构名、机构代码
                    DB db = mapper.selectWithName(connName);
                    if (db == null) {
                        System.out.println("未找到数据库：" + connName);
                        continue;
                    }
                    int id = db.getId();
                    int org = db.getOrg();
                    String dbName = db.getName();
                    String orgCode = db.getOrgCode();
                    Set<String> structs = entry.getValue();
                    // 遍历数据库下表对应的数据集
                    for (String struct : structs) {
                        if (StringUtils.isNotEmpty(struct)) {
                            // 组装字段数据集对照关系url
                            String _refUrl = refUrl.replace("{1}", id + "").replace("{2}", org + "");
                            Struct _struct = mapper.selectIdWithName(struct);
                            if (_struct == null) {
                                System.out.println("未找到数据集：" + struct);
                                continue;
                            }
                            int structId = _struct.getId();
                            String _structName = _struct.getName();
                            _refUrl = _refUrl.replace("{3}", structId + "");
                            System.out.println(_refUrl + "\t" + struct + "\t" + connName);
                            // 请求url获取指定机构、数据库、数据集的对照信息
                            Response response = OkHttpUtil.get(_refUrl);
                            if (response != null && response.isSuccessful()) {
                                // 解析对照信息
                                ResponseBody responseBody = response.body();
                                String string = responseBody.string();
                                JSONObject jsonObject = JSON.parseObject(string);
                                JSONArray jsonArray = jsonObject.getJSONArray("data");
                                if (!(jsonArray.size() > 0)) {
                                    System.out.println(struct + "\t" + connName + " 无对照关系");
                                    continue;
                                }
                                JSONObject _jsonObject = jsonArray.getJSONObject(0);
                                // 解析出对照的数据库表
                                JSONArray dsTables = _jsonObject.getJSONArray("dsTables");
                                for (Object obj : dsTables) {
                                    JSONObject jsonObj = (JSONObject) obj;
                                    // 组装请求生成字段数据集对照sql的参数
                                    String tableName = jsonObj.getString("tableName");
                                    String param = sqlParam.replace("{1}", id + "").replace("{2}", tableName).replace("{3}", structId + "");
                                    // 根据参数请求生成sql接口获取对照后的sql语句
                                    Response _response = OkHttpUtil.post(sqlUrl, param, null);
                                    if (_response != null && _response.isSuccessful()) {
                                        // 处理模板生成sqoop导入脚本
                                        String rb = _response.body().string();
                                        JSONObject rbJSON = JSON.parseObject(rb);
                                        JSONObject data = rbJSON.getJSONObject("data");
                                        String showSQL = data.getString("showSQL");
                                        showSQL = showSQL.replace("\'", "\"");
                                        String _script = script.replace("{1}", dbName);
                                        _script = _script.replace("{2}", _structName);
                                        _script = _script.replace("{3}", showSQL);
                                        _script = _script.replace("{4}", _structName);
                                        // 机构代码
                                        _script = _script.replace("{5}", orgCode);
                                        // 将sqoop脚本写入文件
                                        writer.write(_script);
                                        writer.flush();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
