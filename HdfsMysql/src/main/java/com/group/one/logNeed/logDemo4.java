package com.group.one.logNeed;

import com.group.one.utils.ConnectUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class logDemo4 {
    /*
    需求四：4.统计每日购买次数最多的三个用户的昵称，购买次数,消费金额。
    * */
    public static void logNeed4() {
        //获取连接
        Connection hive_conn = ConnectUtils.getHive_conn();
        Connection mysql_conn = ConnectUtils.getMysql_conn();
//        System.out.println(hive_conn);
//        System.out.println(mysql_conn);
        try {
            PreparedStatement hive_ps = hive_conn.prepareStatement("select * from (select *,row_number()over(partition by a.dt order by a.counts desc) paiming\n" +
                    "from\n" +
                    "(select times dt,user_name,count(product_name) counts,sum(product_price) from Tall_jiexi\n" +
                    "group by times,user_name)a)b\n" +
                    "where b.paiming<=3");
            //返回值
            ResultSet resultSet = hive_ps.executeQuery();
            //获取数据
            while (resultSet.next()) {
                //把获取到的数据存储到mysql
                PreparedStatement mysql_ps = mysql_conn.prepareStatement("insert into logDemo4 values(?,?,?,?,?)");
                mysql_ps.setString(1, resultSet.getString(1));
                mysql_ps.setString(2, resultSet.getString(2));
                mysql_ps.setInt(3, resultSet.getInt(3));
                mysql_ps.setInt(4, resultSet.getInt(4));
                mysql_ps.setInt(5, resultSet.getInt(5));
                //提交结果
                mysql_ps.executeUpdate();

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main( String[] args) {
        logNeed4();

    }

}
