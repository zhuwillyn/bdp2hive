package com.cecdata.bdp2hive.sqoop.util;

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

    public static String get(String key){
        Properties properties = new Properties();
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("info.properties");){
            properties.load(inputStream);
            String property = properties.getProperty(key);
            return property;
        } catch (Exception e){
            e.printStackTrace();
        }
        return "";
    }

}
