package com.utils;

import java.io.IOException;
import java.util.Properties;

/**
 * @author medal
 * @create 2019-03-23 16:20
 **/
public class PropertiesUtil {
   public static Properties properties = null;
   static {
       properties = new Properties();
       try {
           properties.load(ClassLoader.getSystemResourceAsStream("hbase.properties"));
       } catch (IOException e) {
           System.out.println("the properties is load filed!");
       }
   }

   public static String getPropertiesValue(String key){
       return properties.getProperty(key);
   }
}
