package com.group.one.logNeed;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.group.one.utils.ConnectUtils;
public class logDemo1 {
    /*
    需求一：1.统计男生和女生消费的商品总量和用户总数量。
    * */
    public static void logNeed1() {
        //获取连接
        Connection hive_conn = ConnectUtils.getHive_conn();
        Connection mysql_conn = ConnectUtils.getMysql_conn();
//        System.out.println(hive_conn);
//        System.out.println(mysql_conn);
        try {
            PreparedStatement hive_ps = hive_conn.prepareStatement("select user_gender gender,sum(product_price) zonge,count(distinct user_id) use_counts from Tall_jiexi\n" +
                    "group by user_gender");
            //返回值
            ResultSet resultSet = hive_ps.executeQuery();
            //获取数据
            while (resultSet.next()) {
                //把获取到的数据存储到mysql
                PreparedStatement mysql_ps = mysql_conn.prepareStatement("insert into logDemo1 values(?,?,?)");
                mysql_ps.setString(1, resultSet.getString(1));
                mysql_ps.setInt(2, resultSet.getInt(2));
                mysql_ps.setInt(3, resultSet.getInt(3));
                //提交结果
                mysql_ps.executeUpdate();

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void main( String[] args) {
        logNeed1();
    }

}
