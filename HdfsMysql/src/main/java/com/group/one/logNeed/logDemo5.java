package com.group.one.logNeed;

import com.group.one.utils.ConnectUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class logDemo5 {
    /*
    需求五：5.统计消费排名前5用户，贡献了多少额度。
    * */
    public static void logNeed5() {
        //获取连接
        Connection hive_conn = ConnectUtils.getHive_conn();
        Connection mysql_conn = ConnectUtils.getMysql_conn();
//        System.out.println(hive_conn);
//        System.out.println(mysql_conn);
        try {
            PreparedStatement hive_ps = hive_conn.prepareStatement("select round(c.sums/b.sums2,2) from\n" +
                    "(select sum(a.sums) sums from (select user_name,sum(product_price) sums from Tall_jiexi\n" +
                    "group by user_name\n" +
                    "order by sums desc\n" +
                    "limit 5)a)c,\n" +
                    "(select sum(product_price) as sums2 from Tall_jiexi)b");
            //返回值
            ResultSet resultSet = hive_ps.executeQuery();
            //获取数据
            while (resultSet.next()) {
                //把获取到的数据存储到mysql
                PreparedStatement mysql_ps = mysql_conn.prepareStatement("insert into logDemo5 values(?)");
                mysql_ps.setDouble(1, resultSet.getDouble(1));
                //提交结果
                mysql_ps.executeUpdate();

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main( String[] args) {
      logNeed5();

    }

}
