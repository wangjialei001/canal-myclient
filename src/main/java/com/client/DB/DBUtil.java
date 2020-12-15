package com.client.DB;
import java.sql.*;
import java.util.ResourceBundle;

public class DBUtil {
    private static String driverClass;
    private static String url;
    private static String username;
    private static String password;

    //静态代码块加载类时执行一次，加载数据库信息文件
    static{
        //用来加载properties文件的数据， （文件时键值对， 名字要完整匹配）
        ResourceBundle rb = ResourceBundle.getBundle("sqlconfig");//这是properties的文件名
        driverClass = rb.getString("driverClass");
        url = rb.getString("url");
        username = rb.getString("username");
        password = rb.getString("password");
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
    }
    //得到连接的方法
    public static Connection getConnection() throws Exception{
        return DriverManager.getConnection(url,username,password);
    }
    //关闭资源
    public static void closeAll(ResultSet rs , PreparedStatement pre, Connection conn){
        if (rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pre != null){
            try {
                pre.close();
            } catch (SQLException e) {
                // TODO 自动生成的 catch 块
                e.printStackTrace();
            }
        }
        if (conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO 自动生成的 catch 块
                e.printStackTrace();
            }
        }
    }
}
