package com.cecdata.bdp2hive;

import com.cecdata.bdp2hive.hive.GenerateHivePartitionTable;
import com.cecdata.bdp2hive.log.LogApplication;
import com.cecdata.bdp2hive.sqoop.SqoopMainApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * @author zhuweilin
 * @project bdp2hive
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/06/29 15:58
 */
public class MainApplication {

    private static Logger logger = LoggerFactory.getLogger(MainApplication.class);

    public static void main(String[] args) {
        logger.info("start ...");
        // 通过java -jar jar文件 [hive|sqoop]命令执行相关的生成分区表或者生成sqoop脚本程序
        if (args.length < 1) {
            logger.info("输入参数错误");
            System.exit(-1);
        }
        try {
            // 判断子命令进而利用反射执行不同类的启动方法
            if ("hive".equals(args[0].toLowerCase())) {
                logger.info("enter sub command: hive");
                Class<GenerateHivePartitionTable> clazz = GenerateHivePartitionTable.class;
                Method method = clazz.getDeclaredMethod("main", String[].class);
                method.setAccessible(true);
                method.invoke(clazz.newInstance(), new Object[]{args});
            } else if ("sqoop".equals(args[0].toLowerCase())) {
                logger.info("enter sub command: sqoop");
                Class<SqoopMainApplication> clazz = SqoopMainApplication.class;
                Method method = clazz.getDeclaredMethod("main", String[].class);
                method.setAccessible(true);
                method.invoke(clazz.newInstance(), new Object[]{args});
            } else if("log".equals(args[0].toLowerCase())){
                Class<LogApplication> clazz = LogApplication.class;
                Method method = clazz.getDeclaredMethod("main", String[].class);
                method.setAccessible(true);
                method.invoke(clazz.newInstance(), new Object[]{args});
            } else {
                logger.info("please type [hive|sqoop] to start programs");
                System.exit(-1);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

}
