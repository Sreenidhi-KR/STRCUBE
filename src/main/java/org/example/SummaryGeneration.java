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

    public void compareTables(String queryId, String aggregateFunction , ResultSet rsNew) throws SQLException {
        Statement stmtOld = conn.createStatement();
        String QUERY_TABLE_NAME = "QUERY_RESULT_"+queryId;
        ResultSetMetaData rsmd = rsNew.getMetaData();
        StringJoiner columnDefs = new StringJoiner(", ");

        int columnCount = rsmd.getColumnCount();
        String[] columnNames = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            columnNames[i - 1] = rsmd.getColumnName(i);
            String name = rsmd.getColumnName(i);
            String type = rsmd.getColumnTypeName(i);
            int length = rsmd.getColumnDisplaySize(i);
            columnDefs.add(name + " " + type + "(" + length + ")");
        }
        String createTableQuery = "CREATE TABLE IF NOT EXISTS " + QUERY_TABLE_NAME + " ( id INT AUTO_INCREMENT PRIMARY KEY , " + columnDefs.toString() + ")";
        System.out.println(createTableQuery);
        Statement stmtCreate = conn.createStatement();
        stmtCreate.executeUpdate(createTableQuery);

        while (rsNew.next()) {
            String[] values = new String[columnCount];
            for (int i = 0; i < values.length; i++) {
                values[i] = rsNew.getString(i+1);
            }
            System.out.println("SELECT * FROM " + QUERY_TABLE_NAME + " WHERE " + getWhereClause(values, rsNew));
            ResultSet rsOld = stmtOld.executeQuery("SELECT * FROM " + QUERY_TABLE_NAME + " WHERE " + getWhereClause(values, rsNew));
            if (rsOld.next()) {
                System.out.println("Row found in old table: ");
                String result = rsNew.getString("result");
                int id = rsOld.getInt("id");
                //set update condition
                String updateStmt ="";
                if(aggregateFunction.equals("SUM") || aggregateFunction.equals("COUNT")){
                    updateStmt = "Result = Result + ?";
                } else if (aggregateFunction.equals("MIN")) {

                    updateStmt = "Result = LEAST(Result , ?)";
                }
                else if (aggregateFunction.equals("MAX")) {

                    updateStmt = "Result = GREATEST(Result , ?)";
                }
                else if(aggregateFunction.equals("AVG")){
                    //WRONG
                    updateStmt = "Result = ?";
                }
                PreparedStatement stmtUpdate = conn.prepareStatement("UPDATE " + QUERY_TABLE_NAME + " SET " + updateStmt+" WHERE id=?");
                stmtUpdate.setString(1, result);
                stmtUpdate.setInt(2, id);
                System.out.println(stmtUpdate.toString());
                stmtUpdate.executeUpdate();
            } else {
                System.out.println("Row not found in old table: ");
                String insertQuery = "INSERT INTO " + QUERY_TABLE_NAME + " (" + String.join(", ", columnNames) + ") VALUES (";
                for (int i = 0; i < values.length; i++) {
                    insertQuery += "'" + values[i] + "'";
                    if (i < values.length - 1) {
                        insertQuery += ",";
                    }
                }
                insertQuery += ")";
                PreparedStatement stmtInsert = conn.prepareStatement(insertQuery);
                System.out.println(stmtInsert.toString());
                stmtInsert.executeUpdate();
            }
        }
    }

    private String getWhereClause(String[] values , ResultSet rsNew) throws SQLException {
        String whereClause = "";
        for (int i = 0; i < values.length; i++) {
            String columnName = rsNew.getMetaData().getColumnName(i+1);
            if (columnName.equals("result")) {
                continue; // skip "result" column
            }
            whereClause += (whereClause.isEmpty() ? "" : " AND ") + columnName + "='" + values[i] + "'";
        }
        return whereClause;
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
                summaryGeneration.compareTables(queryId , aggregateFunction, rs);
                System.out.println();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                stmt.close();
                conn.close();

            } catch (Exception e) {}

        }

    }
}
