package com.cecdata.bdp2hive.sqoop.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author zhuweilin
 * @project cecdata-platform
 * @description
 * @mail zhuwillyn@163.com
 * @date 2017/12/27 14:36
 */
public class PropertiesUtil {

    static Properties properties = new Properties();

    public static String get(String key){
        try {
            String property = properties.getProperty(key);
            return property;
        } catch (Exception e){
            e.printStackTrace();
        }
        return "";
    }


    static {
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("info.properties");){
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
