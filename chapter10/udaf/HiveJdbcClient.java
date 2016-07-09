package hive.udaf;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;
public class HiveJdbcClient {
        private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
        private static String url = "jdbc:hive://192.168.153.100:10000/default";
        private static String user = "hive";
        private static String password = "hive";
        private static String sql = "";
        private static ResultSet res;
        private static final Logger log = Logger.getLogger(HiveJdbcClient.class);
        public static void main(String[] args) {
                try {
                        Class.forName(driverName);
                        Connection conn = DriverManager.getConnection(url, user, password);
                        Statement stmt = conn.createStatement();
                        // �����ı���
                        String tableName = "testHiveJDBC";
                        /** ��һ��:���ھ���ɾ�� **/
                        sql = "drop table " + tableName;
                        stmt.executeQuery(sql);
                        /** �ڶ���:�����ھʹ��� **/
                        sql = "create table " + tableName + " (key int, value string) row format delimited fields terminated by '\t'";
                        stmt.executeQuery(sql);
                        // ִ�С�show tables������
                        sql = "show tables '" + tableName + "'";
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("ִ�С�show tables�����н��:");
                        if (res.next()) {
                                System.out.println(res.getString(1));
                        }
                        // ִ�С�describe table������
                        sql = "describe " + tableName;
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("ִ�С�describe table�����н��:");
                        while (res.next()) {  
                                System.out.println(res.getString(1) + "\t" + res.getString(2));
                        }

                        // ִ�С�load data into table������
                        String filepath = "/home/hadoop/ziliao/userinfo.txt";
                        sql = "load data local inpath '" + filepath + "' into table " + tableName;
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                           // ִ�С�select * query������
                        sql = "select * from " + tableName;
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("ִ�С�select * query�����н��:");
                        while (res.next()) {
                                System.out.println(res.getInt(1) + "\t" + res.getString(2));
                        }
                        // ִ�С�regular hive query������
                        sql = "select count(1) from " + tableName;
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("ִ�С�regular hive query�����н��:");
                        while (res.next()) {
                                System.out.println(res.getString(1));
                        }
                        conn.close();
                        conn = null;
                } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        log.error(driverName + " not found!", e);
                        System.exit(1);
                } catch (SQLException e) {
                        e.printStackTrace();
                        log.error("Connection error!", e);
                        System.exit(1);
                }

        }
}
