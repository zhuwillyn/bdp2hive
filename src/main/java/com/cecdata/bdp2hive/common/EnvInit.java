package com.cecdata.bdp2hive.common;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author zhuweilin
 * @project bdp2hive
 * @description
 * @mail zhuwillyn@163.com
 * @date 2018/07/03 08:57
 */
public class EnvInit {

    private static ApplicationContext context;

    static {
        context = new ClassPathXmlApplicationContext("applicatioinContext.xml");
    }

    public static ApplicationContext getContext(){
        return context;
    }

    public static Object getBean(String beanName){
        return context.getBean(beanName);
    }


    public static <T> T getBean(Class<T> clazz){
        return context.getBean(clazz);
    }

}
