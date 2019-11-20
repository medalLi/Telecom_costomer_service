package com.utils;

import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryServices;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class PhoenixUtils {

    /**
     * 获取phoenix连接
     * @param phoenixUrl
     * @return
     */
    public static Connection getPhoenixConn(String phoenixUrl){
        Connection conn = null;
        try {
            Properties connProp = new Properties();
            connProp.put("driver", "org.apache.phoenix.jdbc.PhoenixDriver");
            connProp.put("phoenix.query.timeoutMs", "600000");
            connProp.put("hbase.rpc.timeout", "600000");
            connProp.put("hbase.client.scanner.timeout.period", "6000");
            connProp.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,Boolean.toString(true));//开启配置文件
            connProp.put(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.toString(true));//开启配置文件
            connProp.put("phoenix.mutate.batchSize", "200000");//执行过程被批处理的最大行数
            connProp.put("phoenix.mutate.maxSize", "15000000");//客户端批处理的最大行数
            connProp.put("phoenix.mutate.maxSizeBytes", "1048576000");//客户端批处理的最大数据量 1g
            PhoenixDriver phoenixDriver = new PhoenixDriver();
            conn = phoenixDriver.connect(phoenixUrl, connProp);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
