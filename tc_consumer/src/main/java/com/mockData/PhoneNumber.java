package com.mockData;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @author medal
 * @create 2019-03-22 13:16
 **/
public class PhoneNumber {

    static Properties properties = null;
    static File file = null;
    static {
        properties = new Properties();
        file = new File("tc_consumer/src/main/resources/phoneNumber.properties");

        try {
            properties.load(new FileReader(file));
        } catch (IOException e) {
            System.out.println("load the file is filed");
        }
    }

    public static String get(){

        //获取手机号码
        String line = properties.getProperty("pn");
        String[] lineSplit = line.split(",");

        //System.out.println(line);
        String result = "";
        result = lineSplit[(int)(Math.random() * lineSplit.length)];
        return result;
    }
}
