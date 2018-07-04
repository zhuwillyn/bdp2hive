package com.cecdata.bdp2hive;

import java.lang.reflect.Method;

/**
 * @author zhuweilin
 * @project bdp2hive
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/06/29 15:58
 */
public class MainApplication {

    public static void main(String[] args) throws Exception {
        // 通过java -jar jar文件 [hive|sqoop]命令执行相关的生成分区表或者生成sqoop脚本程序
        if (args.length < 1) {
            System.out.println("输入参数错误");
            System.exit(-1);
        }
        // 判断进而利用反射执行不同类的启动方法
        if ("hive".equals(args[0].toLowerCase())) {
            Class<?> clazz = Class.forName("com.cecdata.bdp2hive.hive.GenerateHivePartitionTable");
            Method method = clazz.getDeclaredMethod("main", String[].class);
            method.setAccessible(true);
            method.invoke(clazz.newInstance(), new Object[]{args});
        } else if ("sqoop".equals(args[0].toLowerCase())) {
            Class<?> clazz = Class.forName("com.cecdata.bdp2hive.sqoop.SqoopMainApplication");
            Method method = clazz.getDeclaredMethod("main", String[].class);
            method.setAccessible(true);
            method.invoke(clazz.newInstance(), new Object[]{args});
        } else {
            System.out.println("please type [hive|sqoop] to start programs");
            System.exit(-1);
        }
    }

}
