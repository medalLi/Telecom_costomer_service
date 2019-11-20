package main.java.producer;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @author medal
 * @create 2019-03-22 13:44
 **/
public class CallTime {

    public static String get(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        long startTime = 0;
        long endTime = 0;

        try {
            startTime = sdf.parse("2017-00-00 00:00:00").getTime();
            endTime = sdf.parse("2019-00-00 00:00:00").getTime();
        } catch (ParseException e) {
            System.out.println("the time is parse filed");
        }

        long number = (long)(Math.random()*(endTime-startTime));
        String line = sdf.format(new Date(startTime+number));

        return line;
    }
}
