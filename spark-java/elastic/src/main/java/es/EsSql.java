package es;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author admin 2020-6-17
 */
public class EsSql {
    public static void main(String[] args) {
        String address = "jdbc:es://" + "localhost:9200";
        Properties connectionProperties = new Properties();
        try {
            Connection connection = DriverManager.getConnection(address, connectionProperties);
            System.out.println(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
