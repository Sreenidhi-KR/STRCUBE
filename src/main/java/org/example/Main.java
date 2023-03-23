package org.example;
import java.io.*;
import java.sql.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

public class Main {

    public static void main(String[] args) {
        String csvFile = "/Users/boppanavenkatesh/Desktop/dimensions/product.csv";
        String xmlFile = "/Users/boppanavenkatesh/Desktop/dimensions/meta.xml";
        String tableName = "product";
        String jdbcUrl = "jdbc:mysql://localhost:3306/DataModeling";
        String jdbcUser = "root";
        String jdbcPassword = "KVrsmck@21";

        try {
            // Read the metadata from the XML file
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(new File(xmlFile));
            doc.getDocumentElement().normalize();

            // Get the column names and data types from the XML
            NodeList nodeList = doc.getElementsByTagName("column");
            String[] columnNames = new String[nodeList.getLength()];
            String[] dataTypes = new String[nodeList.getLength()];
            for (int i = 0; i < nodeList.getLength(); i++) {
                Element element = (Element) nodeList.item(i);
                columnNames[i] = element.getAttribute("name");
                dataTypes[i] = element.getAttribute("type");
            }

            // Connect to the MySQL database
            Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);

            // Create the table in MySQL
            Statement statement = connection.createStatement();
            String createTableSql = "CREATE TABLE " + tableName + " (";
            for (int i = 0; i < columnNames.length; i++) {
                createTableSql += columnNames[i] + " " + dataTypes[i];
                if(i==0)
                    createTableSql += " PRIMARY KEY";
                if (i < columnNames.length - 1) {
                    createTableSql += ", ";
                }
            }
            createTableSql += ")";
            statement.executeUpdate(createTableSql);

            // Read the data from the CSV file and insert it into MySQL
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                String insertSql = "INSERT INTO " + tableName + " (";
                for (int i = 0; i < columnNames.length; i++) {
                    insertSql += columnNames[i];
                    if (i < columnNames.length - 1) {
                        insertSql += ", ";
                    }
                }
                insertSql += ") VALUES (";
                for (int i = 0; i < values.length; i++) {
                    if (dataTypes[i].equals("VARCHAR(255)")) {
                        insertSql += "'" + values[i] + "'";
                    } else {
                        insertSql += values[i];
                    }
                    if (i < values.length - 1) {
                        insertSql += ", ";
                    }
                }
                insertSql += ")";
                statement.executeUpdate(insertSql);

            }
            br.close();

            // Close the connection
            connection.close();
            System.out.println("Table Created Successfully as per XML and Data Inserted Successfully as per CSV...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
