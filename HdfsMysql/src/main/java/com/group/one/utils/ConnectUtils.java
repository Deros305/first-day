package com.group.one.utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author: 张鹏飞
 * @company： 软通动力信息技术股份有限公司
 * @Official： www.isoftstone.com
 */
public class ConnectUtils {
    static Connection hive_conn;
    static Connection mysql_conn;
    /**
     *自己手写jdbc了
     */
    static {
        try {
            //配置参数
            Properties properties = new Properties();
            properties.load(ConnectUtils.class.getClassLoader().getResourceAsStream("application.properties"));
            //加载驱动  JDBC
            Class.forName("com.mysql.jdbc.Driver");
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            //获取hie连接对象（处理数据） 跟 mysql连接对象（保存处理结果）
             mysql_conn = DriverManager.getConnection(properties.getProperty("mysql.datasource.url"),properties.getProperty("mysql.datasource.username") ,properties.getProperty("mysql.datasource.password") );
             hive_conn = DriverManager.getConnection(properties.getProperty("hive.datasource.url"),properties.getProperty("hive.datasource.username"),properties.getProperty("hive.datasource.password"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取数据库mysql对象
     */
    public static Connection  getMysql_conn(){
        return mysql_conn;
    }


    /*
    获取数据仓库hive对象
     */
    public static Connection  getHive_conn(){
        return hive_conn;
    }

     /*
    关流
     */
     public static void close(){
         if(hive_conn!=null){
             try {
                 hive_conn.close();
             } catch (SQLException e) {
                 e.printStackTrace();
             }
         }

         if(mysql_conn!=null){
             try {
                 mysql_conn.close();
             } catch (SQLException e) {
                 e.printStackTrace();
             }
         }
     }



}
