package com.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @author medal
 * @create 2019-03-23 18:41
 **/
public class HbaseUtil {

    //判断表是否存在
    public static boolean isExsist(Configuration conf,String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return result;
    }

    //初始化命名空间
    public static void initNameSpace(Configuration conf,String nameSpace) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        NamespaceDescriptor[]  array =admin.listNamespaceDescriptors();

        ArrayList<String> list = new ArrayList<String>();
        for(NamespaceDescriptor nd1 : array){
            list.add(nd1.getName());
        }

        if(!list.contains(nameSpace)){
            NamespaceDescriptor nd = NamespaceDescriptor
                    .create(nameSpace)
                   // .addConfiguration("Author","medal")
                    .addConfiguration("CreateTime",String.valueOf(System.currentTimeMillis()))
                    .build();
           // System.out.println("nameSpace : "+nameSpace);
            admin.createNamespace(nd);
           // System.out.println("hahahhahhahahhhahhaha#####################");
        }

        admin.close();
        connection.close();
    }

    //创建表
    public static void createTable(Configuration conf, String tableName, int regions, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if(isExsist(conf, tableName)) return;

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf: columnFamily){
            htd.addFamily(new HColumnDescriptor(cf));
        }
        //htd.addCoprocessor("CalleeWriteObserver");
        admin.createTable(htd, genSplitKeys(regions));
        admin.close();
        connection.close();
    }

    //预分区
    private static byte[][] genSplitKeys(int regions){
        //定义一个存放分区键的数组
        String[] keys = new String[regions];
        //目前推算，region个数不会超过2位数，所以region分区键格式化为两位数字所代表的字符串
        DecimalFormat df = new DecimalFormat("00");
        for(int i = 0; i < regions; i ++){
            keys[i] = df.format(i) + "|";
        }

        byte[][] splitKeys = new byte[regions][];
        //生成byte[][]类型的分区键的时候，一定要保证分区键是有序的
        TreeSet<byte[]> treeSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for(int i = 0; i < regions; i++){
            treeSet.add(Bytes.toBytes(keys[i]));
        }

        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        int index = 0;
        while(splitKeysIterator.hasNext()){
            byte[] b = splitKeysIterator.next();
            splitKeys[index ++] = b;
        }
        return splitKeys;
    }

    //生成rowKey
    public static String genRowKey(String regionCode, String call1, String buildTime, String call2, String flag, String duration){
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(call1 + "_")
                .append(buildTime + "_")
                .append(call2 + "_")
                .append(flag + "_")
                .append(duration);
        return sb.toString();
    }

    //生成分区号
    public static String genRegionCode(String call1, String buildTime, int regions){
        int len = call1.length();
        //取出后4位号码
        String lastPhone = call1.substring(len - 4);
        //取出年月
        String ym = buildTime
                .replaceAll("-", "")
                .replaceAll(":", "")
                .replaceAll(" ", "")
                .substring(0, 6);
        //离散操作1
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        int a = 10;
        int b = 20;
        a = a ^ b;
        b = a ^ b;
        a = a ^ b;
        //离散操作2
        int y = x.hashCode();
        //生成分区号
        int regionCode = y % regions;
        //格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return  df.format(regionCode);
    }
}
