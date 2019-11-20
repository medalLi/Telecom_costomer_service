package main.java.producer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Date;

/**
 * @author medal
 * @create 2019-03-22 13:13
 **/
/*
生产数据，数据格式为：
    15714728273,15422018558,2017-02-10 18:52:08,0718
    17764278604,15897468949,2017-06-10 09:20:05,0595
    15978226424,15711910344,2017-09-09 05:42:57,0188
    15542823911,15897468949,2017-09-13 17:45:38,0603
    18576581848,15897468949,2017-08-07 16:12:40,0241

第一个字段为：主叫号码
第二个字段为：被叫号码
第三个字段为：通话时间
第四个字段为：通话时长

经过测试：
    10秒钟可以写5.5M左右
* */
public class ProducerLog {
    public static void main(String[] args)throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter("/newDisk/test/myProject/Telecom_costomer_service/ProducerLog.csv"));

        String line1 = "";
        String line2 = "";
        String line3 = "";
        String line4 = "";
        StringBuffer sb = new StringBuffer();
//        long startTime = System.currentTimeMillis();
//        long endTime = System.currentTimeMillis();
//        System.out.println(new Date(startTime));
        while (true){
            //System.out.println(startTime);
            line1 = PhoneNumber.get();
            line2 = PhoneNumber.get();
            line3 = CallTime.get();
            line4 = CallDuration.get();

            while(line1.equals(line2)){
                line2 = PhoneNumber.get();
            }
            sb.append(line1).append(",").append(line2).append(",").
                    append(line3).append(",").append(line4);
            //System.out.println(new String(sb));
            bw.write(new String(sb));
            bw.newLine();
            bw.flush();
            sb.delete(0,sb.length());
            Thread.sleep(100);
           // endTime = System.currentTimeMillis();
        }
        //System.out.println(new Date(endTime));
    }
}
