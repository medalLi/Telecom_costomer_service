package com.hbase;

/**
 * @author medal
 * @create 2019-03-23 18:41
 **/

import com.utils.ConnectionInstance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import com.utils.HbaseUtil;
import com.utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class HbaseDao {
    private int regions;
    private String namespace;
    private String tableName;
    public static final Configuration conf;
    private HTable table;
    private Connection connection;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    //优化hbase
    private ArrayList<Put> cacheList1 = new ArrayList<Put>();
    private ArrayList<Put> cacheList2 = new ArrayList<Put>();
    static {
        conf = HBaseConfiguration.create();
    }

    public HbaseDao() {

        try {
            regions = Integer.valueOf(PropertiesUtil.getPropertiesValue("hbase.calllog.regions"));
            namespace = PropertiesUtil.getPropertiesValue("hbase.calllog.namespace");
            tableName = PropertiesUtil.getPropertiesValue("hbase.calllog.tablename");

            connection = ConnectionFactory.createConnection(conf);

            if (!HbaseUtil.isExsist(conf, tableName)) {
                HbaseUtil.initNameSpace(conf, namespace);
                HbaseUtil.createTable(conf, tableName, regions, "f1", "f2");
            }


//            table = (HTable)connection.getTable(TableName.valueOf(tableName));
//            // 优化hbase
//            ((HTable) table).setAutoFlush(false);
//            table.setWriteBufferSize(4*1024*1024);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * ori数据样式： 18576581848,17269452013,2017-08-14 13:38:31,1761
     * rowkey样式：01_18576581848_20170814133831_17269452013_1_1761
     * HBase表的列：call1  call2   build_time   build_time_ts   flag   duration
     * @param ori
     */
    public void put(String ori) {
        try {
            if(cacheList1.size() == 0){
                connection = ConnectionInstance.getConnection(conf);
                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                table.setAutoFlushTo(false);
                table.setWriteBufferSize(2 * 1024 * 1024);
            }

            String[] splitOri = ori.split(",");

            String caller = splitOri[0];
            String callee = splitOri[1];
            String buildTime = splitOri[2];
            String duration = splitOri[3];

            String regionCode = HbaseUtil.genRegionCode(caller, buildTime, regions);

            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));
            String buildTimeTs = String.valueOf(sdf1.parse(buildTime).getTime());

            //生成rowkey
            String rowkey = HbaseUtil.genRowKey(regionCode, caller, buildTimeReplace, callee, "1", duration);

            //向表中插入该条数据
            Put put1 = new Put(Bytes.toBytes(rowkey));
            put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call1"), Bytes.toBytes(caller));
            put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call2"), Bytes.toBytes(callee));
            put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
            put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs));
            put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
            put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

            Put put2 = new Put(Bytes.toBytes(rowkey));
            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs));
            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));
            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

            //table.put(put1);
            //table.put(put2);
            //System.out.println("put seccess!!!");
            cacheList1.add(put1);
            cacheList2.add(put2);

            if(cacheList1.size() >= 30){
                table.put(cacheList1);
                table.put(cacheList2);
                table.flushCommits();

                table.close();
                cacheList1.clear();
                cacheList2.clear();
                System.out.println("hahahhah@@@@@@@@@@@@@@");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}

