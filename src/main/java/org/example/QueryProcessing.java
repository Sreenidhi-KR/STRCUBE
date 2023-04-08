package org.example;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class QueryProcessing {
    DBConfig dbConfig=new DBConfig();
    String url = dbConfig.getUrl();
    String username = dbConfig.getUsername();
    String password = dbConfig.getPassword();

    public QueryProcessing() {
    }

    public void GenerateSummary(){
        try{
            File inputFile = new File("./dimensions/DMInstance.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();

            NodeList groupItemList = doc.getElementsByTagName("GroupItem");
            String[] groupcolumnNames = new String[groupItemList.getLength()];
            String[] groupcolumnTypes = new String[groupItemList.getLength()];
            String[] groupcolumnProperties = new String[groupItemList.getLength()];

            for(int i=0;i<groupItemList.getLength();i++){
                Element summaryItem = (Element) groupItemList.item(i);
                groupcolumnNames[i] = summaryItem.getTextContent();
                groupcolumnTypes[i]=summaryItem.getAttribute("type");
                groupcolumnProperties[i]=summaryItem.getAttribute("property");
//                System.out.println(groupcolumnNames[i] + groupcolumnTypes[i]+ groupcolumnProperties[i]);
            }

            String groupsql = "CREATE TABLE GroupByMapping" + " (";
            for (int j = 0; j < groupcolumnNames.length; j++) {
                groupsql += groupcolumnNames[j] + " " + groupcolumnTypes[j] + " " + groupcolumnProperties[j];
                if (j < groupcolumnNames.length - 1) {
                    groupsql += ", ";
                }
            }
            groupsql += ")";
//            System.out.println(groupsql);



            NodeList summaryItemList = doc.getElementsByTagName("SummaryItem");
            String[] columnNames = new String[summaryItemList.getLength()];
            String[] columnTypes = new String[summaryItemList.getLength()];
            String[] columnProperties = new String[summaryItemList.getLength()];

            for(int i=0;i<summaryItemList.getLength();i++){
                Element summaryItem = (Element) summaryItemList.item(i);
                columnNames[i] = summaryItem.getTextContent();
                columnTypes[i]=summaryItem.getAttribute("type");
                columnProperties[i]=summaryItem.getAttribute("property");
//                System.out.println(columnNames[i] + columnTypes[i]+ columnProperties[i]);
            }



            String sql = "CREATE TABLE Summary" + " (";
            for (int j = 0; j < columnNames.length; j++) {
                sql += columnNames[j] + " " + columnTypes[j] + " " + columnProperties[j];
                if (j < columnNames.length - 1) {
                sql += ", ";
                }
            }
            sql += ")";
//            System.out.println(sql);

//            String altersql="alter table Summary add constraint fk_group_id FOREIGN KEY (Group_id) REFERENCES GroupByMapping(id)";


            NodeList logItemList = doc.getElementsByTagName("LogItem");
            String[] logcolumnNames = new String[logItemList.getLength()];
            String[] logcolumnTypes = new String[logItemList.getLength()];
            String[] logcolumnProperties = new String[logItemList.getLength()];

            for(int i=0;i<logItemList.getLength();i++){
                Element logItem = (Element) logItemList.item(i);
                logcolumnNames[i] = logItem.getTextContent();
                logcolumnTypes[i]=logItem.getAttribute("type");
                logcolumnProperties[i]=logItem.getAttribute("property");
//                System.out.println(logcolumnNames[i] + logcolumnTypes[i]+ logcolumnProperties[i]);
            }



            String logsql = "CREATE TABLE Logs" + " (";
            for (int j = 0; j < logcolumnNames.length; j++) {
                logsql += logcolumnNames[j] + " " + logcolumnTypes[j] + " " + logcolumnProperties[j];
                if (j < logcolumnNames.length - 1) {
                    logsql += ", ";
                }
            }
            logsql += ")";
//            System.out.println(logsql);
            String alterlogsql="alter table Logs add constraint fk_query_id FOREIGN KEY (Query_Id) REFERENCES Summary(Query_Id)";


            NodeList queryList = doc.getElementsByTagName("Query");
            String[] queryIdList = new String[queryList.getLength()];
            String[] queryAggregateFunctionList = new String[queryList.getLength()];
            String[] queryFactVariableList= new String[queryList.getLength()];
            boolean[] hasGroupBy= new boolean[queryList.getLength()];

            for(int i=0;i<queryList.getLength();i++){
                Element query = (Element) queryList.item(i);
                queryIdList[i]=query.getAttribute("id");
                queryFactVariableList[i] = query.getElementsByTagName("FactVariable").item(0).getTextContent();
                queryAggregateFunctionList[i] = query.getElementsByTagName("AggregateFunction").item(0).getTextContent();
                if(query.getElementsByTagName("GroupBy").getLength()>0)
                    hasGroupBy[i]=true;
                else
                    hasGroupBy[i]=false;
//                System.out.println(queryIdList[i] +" "+queryFactVariableList[i]+" "+ queryAggregateFunctionList[i]+" "+hasGroupBy[i]);
            }


            String insert[] =new String[queryList.getLength()];
            String header="";
            for(int i=0;i<columnNames.length-2;i++){
                header+=columnNames[i];
                if(i<columnNames.length-3)
                    header+=",";
            }
//            System.out.println(header);
            int groupById=1;
            for(int i=0;i<insert.length;i++){
                if(!hasGroupBy[i])
                    insert[i]="INSERT INTO Summary ("+header+") VALUES ("+queryIdList[i]+",'"+queryFactVariableList[i]+"','"+queryAggregateFunctionList[i]+"')";
                else {
                    insert[i] = "INSERT INTO Summary (" + header + ",Group_Id) VALUES (" + queryIdList[i] + ",'" + queryFactVariableList[i] + "','" + queryAggregateFunctionList[i] + "'," + groupById + ")";
                groupById++;
                }
//                    System.out.println(insert[i]);
            }


            List<String> insertGroupBy=new ArrayList<>();
            List<String> createTable=new ArrayList<>();
            groupById=1;
            String groupHeader="";
            for(int i=1;i<groupItemList.getLength();i++){
                groupHeader+=groupcolumnNames[i];
                if(i<groupItemList.getLength()-1){
                    groupHeader+=",";
                }
            }
//            System.out.println(groupHeader);
            for(int i=0;i<hasGroupBy.length;i++){
                if(hasGroupBy[i]){
                    Element element=(Element) queryList.item(i);
                    Element groupBy=(Element) element.getElementsByTagName("GroupBy").item(0);
                    NodeList listOfGroup=groupBy.getElementsByTagName("Group");
                    String temp="Create Table GroupByResultQueryId_"+element.getAttribute("id")+" (";
                    for (int j=0;j<listOfGroup.getLength();j++){
                        insertGroupBy.add("INSERT INTO GroupByMapping ("+groupHeader+") VALUES ("+groupById+",'"+listOfGroup.item(j).getTextContent()+"')");
                        temp+=listOfGroup.item(j).getTextContent()+" VARCHAR(255) NOT NULL";
                            temp+=",";
                        }
                    temp+=" RESULT DECIMAL DEFAULT 0)";
                    createTable.add(temp);

                    groupById++;
                }
            }

//            for(String s:insertGroupBy){
//                System.out.println(s);
//            }   for(String s:createTable){
//                System.out.println(s);
//            }


            try (Connection conn = DriverManager.getConnection(url, username, password);
                 Statement stmt = conn.createStatement()) {

                stmt.executeUpdate(groupsql);
                System.out.println("[ Table GroupByMapping created successfully... ]");

                stmt.executeUpdate(sql);
                System.out.println("[ Table Summary created successfully... ]");

//                stmt.executeUpdate(altersql);
                stmt.executeUpdate(logsql);
                System.out.println("[ Table Log created successfully... ]");

                stmt.executeUpdate(alterlogsql);
                for(int i=0;i<insert.length;i++){
                    System.out.println(insert[i]);
                    stmt.executeUpdate(insert[i]);
                }
                for(int i=0;i<insertGroupBy.size();i++){
                    System.out.println(insertGroupBy.get(i));
                    stmt.executeUpdate(insertGroupBy.get(i));
                }
                for(int i=0;i<createTable.size();i++){
                    System.out.println(createTable.get(i));
                    stmt.executeUpdate(createTable.get(i));
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }
        catch (Exception e){
            e.printStackTrace();
        }



    }
}
