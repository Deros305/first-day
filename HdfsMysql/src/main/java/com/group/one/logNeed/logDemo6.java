package com.group.one.logNeed;

import com.group.one.utils.ConnectUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class logDemo6 {
    /*
    需求六：6.统计消费排名前6的同学都购买了哪些相同的商品
    * */
    public static void logNeed6() {
        //获取连接
        Connection hive_conn = ConnectUtils.getHive_conn();
        Connection mysql_conn = ConnectUtils.getMysql_conn();
//        System.out.println(hive_conn);
//        System.out.println(mysql_conn);
        try {
            PreparedStatement hive_ps = hive_conn.prepareStatement("select * from (select c.btitle,count(*)counts from\n" +
                    "(select b.user_name bname,b.product_name btitle from\n" +
                    "(select user_name,sum(product_price) zonge from Tall_jiexi\n" +
                    "group by user_name order by zonge desc\n" +
                    "limit 6)a\n" +
                    "join Tall_jiexi b\n" +
                    "on a.user_name=b.user_name\n" +
                    "group by b.user_name,b.product_name)c\n" +
                    "group by c.btitle)d\n" +
                    "where d.counts=6");
            //返回值
            ResultSet resultSet = hive_ps.executeQuery();
            //获取数据
            while (resultSet.next()) {
                //把获取到的数据存储到mysql
                PreparedStatement mysql_ps = mysql_conn.prepareStatement("insert into logDemo6 values(?,?)");
                mysql_ps.setString(1, resultSet.getString(1));
                mysql_ps.setInt(2, resultSet.getInt(2));
                //提交结果
                mysql_ps.executeUpdate();

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main( String[] args) {
      logNeed6();

    }

}
