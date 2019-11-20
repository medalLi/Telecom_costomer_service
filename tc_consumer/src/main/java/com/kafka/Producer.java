package com.kafka;

import com.mockData.CallDuration;
import com.mockData.CallTime;
import com.mockData.PhoneNumber;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.load(Producer.class.getClassLoader().getResourceAsStream("producer.properties"));
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);

        System.out.println("111");
        String line1 = "";
        String line2 = "";
        String line3 = "";
        String line4 = "";
        StringBuffer sb = new StringBuffer();

        //发送数据 send
        ProducerRecord<Integer, String> record = null;

        while (true) {
            //System.out.println(startTime);
            line1 = PhoneNumber.get();
            line2 = PhoneNumber.get();
            line3 = CallTime.get();
            line4 = CallDuration.get();

            while (line1.equals(line2)) {
                line2 = PhoneNumber.get();
            }
            sb.append(line1).append(",").append(line2).append(",").
                    append(line3).append(",").append(line4);
            String data = sb.substring(0, sb.length() - 1);
            record = new ProducerRecord<Integer, String>("telecom", data);
            Thread.sleep(1000);
            producer.send(record);
            sb.delete(0,sb.length()-1);
        }

       // producer.close();
    }
}
