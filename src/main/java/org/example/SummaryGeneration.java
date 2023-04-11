package org.example;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class SummaryGeneration {
    public static DBConfig dbConfig=new DBConfig();
    public static String url = dbConfig.getUrl();
    public static String username = dbConfig.getUsername();
    public static String password = dbConfig.getPassword();

    public SummaryGeneration() {
    }

    public SummaryGeneration(Connection conn) {
        this.conn = conn;
    }

    private Connection conn;

    public void updateQueryResult(String queryId, String aggregateFunction , ResultSet rs , Timestamp timestamp) throws SQLException {
        Statement stmt = conn.createStatement();
        String QUERY_TABLE_NAME = "QUERY_RESULT_"+queryId;
        ResultSetMetaData rsMetaData = rs.getMetaData();
        StringJoiner columnDefs = new StringJoiner(", ");

        int columnCount = rsMetaData.getColumnCount();
        String[] columnNames = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            columnNames[i - 1] = rsMetaData.getColumnName(i);
            String name = rsMetaData.getColumnName(i);
            String type = rsMetaData.getColumnTypeName(i);
            int length = rsMetaData.getColumnDisplaySize(i);
            columnDefs.add(name + " " + type + "(" + length + ")");
        }
        String createTableQuery = "CREATE TABLE IF NOT EXISTS " + QUERY_TABLE_NAME + " ( id INT AUTO_INCREMENT PRIMARY KEY , " + columnDefs + ")";
        System.out.println(createTableQuery);
        Statement stmtCreate = conn.createStatement();
        stmtCreate.executeUpdate(createTableQuery);

        while (rs.next()) {
            String[] values = new String[columnCount];
            for (int i = 0; i < values.length; i++) {
                values[i] = rs.getString(i+1);
            }
            System.out.println("SELECT * FROM " + QUERY_TABLE_NAME + " WHERE " + getWhereClause(values, rs));
            ResultSet rsOld = stmt.executeQuery("SELECT * FROM " + QUERY_TABLE_NAME + " WHERE " + getWhereClause(values, rs));
            if (rsOld.next()) {
                System.out.println("Row found in query table: ");
                String result = rs.getString("result");
                int id = rsOld.getInt("id");
                String updateStmt ="";
                switch (aggregateFunction) {
                    case "SUM", "COUNT" -> updateStmt = "Result = Result + ?";
                    case "MIN" -> updateStmt = "Result = LEAST(Result , ?)";
                    case "MAX" -> updateStmt = "Result = GREATEST(Result , ?)";
                    case "AVG" -> {
                        //TODO
                        String todo;
                        updateStmt = "Result = ?";
                    }
                }
                PreparedStatement stmtUpdate = conn.prepareStatement("UPDATE " + QUERY_TABLE_NAME + " SET " + updateStmt+" WHERE id=?");
                stmtUpdate.setString(1, result);
                stmtUpdate.setInt(2, id);
                System.out.println(stmtUpdate);
                stmtUpdate.executeUpdate();
            } else {
                System.out.println("Row not found in query table: ");
                StringBuilder insertQuery = new StringBuilder("INSERT INTO " + QUERY_TABLE_NAME + " (" + String.join(", ", columnNames) + ") VALUES (");
                for (int i = 0; i < values.length; i++) {
                    insertQuery.append("'").append(values[i]).append("'");
                    if (i < values.length - 1) {
                        insertQuery.append(",");
                    }
                }
                insertQuery.append(")");
                PreparedStatement stmtInsert = conn.prepareStatement(insertQuery.toString());
                System.out.println(stmtInsert.toString());
                stmtInsert.executeUpdate();

            }
        }
        generateLog(queryId,timestamp);
    }


    public void generateLog(String queryId, Timestamp timestamp) throws SQLException {
        String QUERY_TABLE_NAME = "QUERY_RESULT_"+queryId;
        String LOG_TABLE_NAME = "QUERY_LOG_"+queryId;
        DatabaseMetaData metadata = conn.getMetaData();
        ResultSet resultSet = metadata.getTables(null, null, LOG_TABLE_NAME, null);
        boolean tableExists = resultSet.next();
        if(!tableExists){
            String createTableQuery = "CREATE TABLE IF NOT EXISTS " + LOG_TABLE_NAME + " AS SELECT * , ? as timestamp FROM "+QUERY_TABLE_NAME;
            System.out.println(createTableQuery);
            PreparedStatement prepStmt = conn.prepareStatement(createTableQuery);
            prepStmt.setTimestamp(1, timestamp);
            prepStmt.executeUpdate();
            return;
        }

        String updateTableQuery = "INSERT INTO " + LOG_TABLE_NAME + "  SELECT * , ? as timestamp FROM "+QUERY_TABLE_NAME;
        System.out.println(updateTableQuery);
        PreparedStatement prepStmt = conn.prepareStatement(updateTableQuery);
        prepStmt.setTimestamp(1, timestamp);
        prepStmt.executeUpdate();

    }



    private String getWhereClause(String[] values , ResultSet rsNew) throws SQLException {
        StringBuilder whereClause = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            String columnName = rsNew.getMetaData().getColumnName(i+1);
            if (columnName.equals("result")) {
                continue; // skip "result" column
            }
            whereClause.append((whereClause.length() == 0) ? "" : " AND ").append(columnName).append("='").append(values[i]).append("'");
        }
        return whereClause.toString();
    }

    public static List<Element> getQueries() {
        List<Element> queries = new ArrayList<>();
        try {
            File inputFile = new File("./dimensions/DMInstance.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();

            NodeList queryNodes = doc.getElementsByTagName("Query");

            for (int i = 0; i < queryNodes.getLength(); i++) {
                Element queryElem = (Element) queryNodes.item(i);
                Element queryRepoElem = (Element) queryElem.getParentNode();
                String queryRepoType = queryRepoElem.getAttribute("type");
                if (queryRepoType.equals("generic")) {
                    queries.add(queryElem);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queries;
    }


    public static void generateSummary() {
        ResultSet rs = null;
        Statement stmt = null;
        Connection conn = null;
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        try{
            conn = DriverManager.getConnection(url, username, password);
            SummaryGeneration summaryGeneration = new SummaryGeneration(conn);
            stmt = conn.createStatement();
            List<Element> queries = getQueries();
            for (Element query : queries) {
                String queryId = query.getAttribute("id");
                String aggregateFunction = query.getElementsByTagName("AggregateFunction").item(0).getTextContent();
                String factVariable = query.getElementsByTagName("FactVariable").item(0).getTextContent();
                String queryScript = query.getElementsByTagName("QueryScript").item(0).getTextContent();
                System.out.println("Query ID: " + queryId);
                System.out.println("Aggregate Function: " + aggregateFunction);
                System.out.println("Fact Variable: " + factVariable);
                System.out.println("Query Script: " + queryScript);
                rs = stmt.executeQuery(queryScript);
                summaryGeneration.updateQueryResult(queryId , aggregateFunction, rs , timestamp);
                System.out.println();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                assert rs != null;
                rs.close();
                stmt.close();
                conn.close();
            } catch (Exception ignored) {

            }

        }

    }
}
