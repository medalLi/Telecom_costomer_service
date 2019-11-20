package com.kafka;

import com.hbase.HbaseDao;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author medal
 * @create 2019-03-22 16:44
 * //  HbaseDao hd = new HbaseDao();
 *     //hd.put(record.value());
 **/
public class MyKafkaConsumer{
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.load(MyKafkaConsumer.class.getClassLoader().getResourceAsStream("consumer.properties"));
        Consumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(properties);
       // String topic = "";

        //订阅某些topic
        consumer.subscribe(Arrays.asList("telecom".split(",")));
        HbaseDao hd = new HbaseDao();

        //从kafka中拉取数据
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(1000);

            for (ConsumerRecord<Integer, String> record : records) {
                hd.put(record.value());
//                String topic = record.topic();
//                int partition = record.partition();
//                Integer key = record.key();
//                String value = record.value();
//                long offset = record.offset();
//                System.out.println(String.format(
//                        "topic: %s\tpartition: %d\toffset: %d\tkey: %d\tvalue: %s",
//                        topic,
//                        partition,
//                        offset,
//                        key,
//                        value
//                ));
            }
        }
    }
}
