package utils;

import java.sql.*;

/**
 * @author medal
 * @create 2019-03-26 15:23
 **/
public class JDBCUtil {
    //定义JDBC连接器实例化所需要的参数
    public static String MYSQL_DRIVER_CLASS="com.mysql.jdbc.Driver";
    public static String MYSQL_URL="jdbc:mysql://hadoop.spark.com:3306/db_telecom?useUnicode=true&characterEncoding=UTF-8";
    public static String MYSQL_USERNAME = "root";
    public static String MYSQL_PASSWORD = "centos";

    //实例化JDBC连接器对象
    public static Connection getConnection(){
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            return DriverManager.getConnection(MYSQL_URL);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    //关闭资源
    public static void close(Connection conn, Statement state, ResultSet rs){
        try {
            if(rs != null && !rs.isClosed()){
                rs.close();
            }

            if(state != null && !state.isClosed()){
                state.close();
            }

            if(conn != null && !conn.isClosed()){
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
