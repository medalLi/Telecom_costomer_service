package utils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author medal
 * @create 2019-03-26 15:38
 **/
public class JDBCInstance {
    private static Connection conn = null;
    private JDBCInstance(){}

    public static Connection getInstance(){
        try {
            if(conn == null || conn.isClosed() || conn.isValid(3)){
                conn= JDBCUtil.getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
