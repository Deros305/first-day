package com.group.one.logNeed;

import com.group.one.utils.ConnectUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class logDemo3 {
    /*
    需求三：3.统计2022-6-14购买鞋的购买用户,和每人买鞋的数量以及消费的金额。
    * */
    public static void logNeed3() {
        //获取连接
        Connection hive_conn = ConnectUtils.getHive_conn();
        Connection mysql_conn = ConnectUtils.getMysql_conn();
//        System.out.println(hive_conn);
//        System.out.println(mysql_conn);
        try {
            PreparedStatement hive_ps = hive_conn.prepareStatement("select user_name name,count(product_name) sums,sum(product_price) zonge from Tall_jiexi\n" +
                    "where times='2022-06-14' and product_name like \"%鞋%\"\n" +
                    "group by user_name\n" +
                    "order by zonge desc");
            //返回值
            ResultSet resultSet = hive_ps.executeQuery();
            //获取数据
            while (resultSet.next()) {
                //把获取到的数据存储到mysql
                PreparedStatement mysql_ps = mysql_conn.prepareStatement("insert into logDemo3 values(?,?,?)");
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
         logNeed3();

    }

}
