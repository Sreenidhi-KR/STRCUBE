package org.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;

public class SummaryGeneration {
    public static DBConfig dbConfig=new DBConfig();
    public static String url = dbConfig.getUrl();
    public static String username = dbConfig.getUsername();
    public static String password = dbConfig.getPassword();
    public static void generateSummary(int noOfNewRows) {


        ResultSet rs = null;
        ResultSet r = null;
        ResultSet groupByColumnsRs = null;
        Statement stmt = null;
        Statement stmt2 = null;
        Connection conn = null;
        try{
            // Establish a connection to the database
             conn = DriverManager.getConnection(url, username, password);
             stmt = conn.createStatement();
             rs = stmt.executeQuery("SELECT * FROM Summary");

            while (rs.next()) {
                int queryId = rs.getInt("Query_Id");
                String factVariable = rs.getString("Fact_Variable");
                String aggregateFunction = rs.getString("Aggregate_Function");
                String joinTable = rs.getString("Table_Name");
                int groupId = rs.getObject("Group_Id") != null ? rs.getInt("Group_Id") : -1;
                boolean isGrouped = false;
                if (groupId != -1)
                    isGrouped = true;
                Double result = rs.getDouble("Result");

                System.out.println(queryId + ", " + factVariable + ", " + aggregateFunction + ", " + groupId + ", " + result);

                if (isGrouped) {

                    Statement groupStmt = conn.createStatement();
                    Statement groupStmt2 = conn.createStatement();
                    StringBuilder groupByColumns = new StringBuilder("");
                    groupByColumnsRs = groupStmt.executeQuery("SELECT Attribute from GroupByMapping where Group_Id = " +groupId);
                    while (groupByColumnsRs.next()){
                        groupByColumns.append(groupByColumnsRs.getString("Attribute")).append(" ,");
                    }
                    groupByColumns.deleteCharAt(groupByColumns.length() - 1);
                    String queryStmt = "with mergetable as (select * , ROW_NUMBER() OVER () AS rn From FactTable natural join " + joinTable + " order by rn desc limit "+ noOfNewRows+ " ) select "+ groupByColumns.toString()+ " , " +  aggregateFunction + "(" + factVariable + ")   from mergetable group by " + groupByColumns.toString() + " ;";
                    System.out.println(queryStmt);
                    ResultSet queryRs = null;
                    queryRs = groupStmt2.executeQuery(queryStmt);
                    String updateStmt = " ";
                    String table = "GroupByResultQueryId_" +queryId;
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

                    ResultSetMetaData metaData = queryRs.getMetaData();
                    int numColumns = metaData.getColumnCount();
                    StringBuilder whereClause = new StringBuilder();

                    for (int i = 1; i <= numColumns; i++) {
                        String columnName = metaData.getColumnName(i);
                        // Check if the column should be included in the WHERE clause
                        if (!columnName.equals(aggregateFunction+"("+factVariable+")")) {
                            if (whereClause.length() > 0) {
                                whereClause.append(" AND ");
                            }
                            whereClause.append(columnName).append(" = ?");
                        }
                    }
//
                    String updateQuery = "UPDATE " + table+ " SET " + updateStmt + " WHERE " + whereClause.toString();
                    PreparedStatement statement = conn.prepareStatement(updateQuery);

                    while (queryRs.next()) {

                        statement.setObject(1,queryRs.getString(aggregateFunction+"("+factVariable+")"));
                        // Set the values for the columns used in the WHERE clause
                        for (int i = 1; i <= numColumns-1; i++) {
                            statement.setObject(i+1, queryRs.getString(i));
                        }
                        System.out.println("#################################################");
                        System.out.println(statement.toString());
                        statement.addBatch();
                    }

                    statement.executeBatch();
                } else {
                    stmt2 = conn.createStatement();
                    String insertStmt = "SELECT " + aggregateFunction + "(" + factVariable + ") FROM ( SELECT *, ROW_NUMBER() OVER () AS rn FROM FactTable ORDER BY rn DESC limit " + noOfNewRows + ") result";
                    r = stmt2.executeQuery(insertStmt);
                    if (r.next()) {
                        double resultValue = r.getDouble(1);
                        System.out.println("Result value: " + resultValue);
                        String updateStmt = " ";
                        if(aggregateFunction.equals("SUM") || aggregateFunction.equals("COUNT")){
                             updateStmt = "UPDATE Summary SET Result = Result + ? WHERE Query_Id = ?";
                        } else if (aggregateFunction.equals("MIN")) {

                            updateStmt = "UPDATE Summary SET Result = LEAST(Result , ?) WHERE Query_Id = ?";
                        }
                        else if (aggregateFunction.equals("MAX")) {

                            updateStmt = "UPDATE Summary SET Result = GREATEST(Result , ?) WHERE Query_Id = ?";
                        }
                        else if(aggregateFunction.equals("AVG")){
                            //WRONG
                            updateStmt = "UPDATE Summary SET Result = ? WHERE Query_Id = ?";
                        }

                        PreparedStatement pstmt = conn.prepareStatement(updateStmt);
                        pstmt.setDouble(1, resultValue);
                        pstmt.setString(2, String.valueOf(queryId));
                        System.out.println(pstmt.toString());
                        pstmt.executeUpdate();
                    }


                }


            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Close the resources in reverse order of their creation
            try {
                rs.close();
                r.close();
                stmt.close();
                stmt.close();
                conn.close();

            } catch (Exception e) { /* ignored */ }

        }

    }
}
