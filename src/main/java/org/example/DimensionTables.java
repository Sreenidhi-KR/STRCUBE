package org.example;

import java.sql.*;

public class DimensionTables {

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/DataModeling";
        String username = "root"; // replace with your username
        String password = "KVrsmck@21"; // replace with your password
        String sql = "CREATE TABLE DimensionTables " +
                "(id INT NOT NULL AUTO_INCREMENT, " +
                " DimensionTable VARCHAR(255) NOT NULL, " +
                " Pri_Key VARCHAR(255), " +
                " PRIMARY KEY ( id ))";
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            System.out.println("Table created successfully");
        } catch (SQLException e) {
            System.out.println(sql+" "+e.getMessage());
        }
    }
}

