package com.shelbin.ml.algorithm.classification;
import java.sql.*;

public class CkSchema {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:clickhouse://localhost:8123/ai";
        String user = "default";
        String password = "root";
        Connection connection = DriverManager.getConnection(url, user, password);

        String sql = "SELECT * FROM housing LIMIT 1";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            String columnType = metaData.getColumnTypeName(i);
            System.out.println("Column name: " + columnName);
            System.out.println("Column type: " + columnType);
        }

        resultSet.close();
        statement.close();
        connection.close();
    }
}
