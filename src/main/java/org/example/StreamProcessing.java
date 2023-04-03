 package org.example;


import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Collections;

public class StreamProcessing {

    public static void start(String factsDir ,String dimensionDir, String xmlFileName) throws InterruptedException, IOException, ClassNotFoundException, SQLException {
        int windowSize = 0;
        int windowVelocity = 0;
        int windowClockTickInMillis = 0;
        String csvPath;
        File csvFile = null;
        long lastLineOffsetNew = 0;
        int noOfRowsToBeDeleted =  0;
        boolean initialRead = true;


        DBConfig dbConfig=new DBConfig();
        String url = dbConfig.getUrl();
        String username = dbConfig.getUsername();
        String password = dbConfig.getPassword();
        // Set up the database connection
        Connection conn = DriverManager.getConnection(url,username,password);


        try {
            File inputFile = new File("./dimensions/DMInstance.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();
            NodeList dataSourceList = doc.getElementsByTagName("DataSource");
            for (int i = 0; i < dataSourceList.getLength(); i++) {
                Element dataSource = (Element) dataSourceList.item(i);
                String size = dataSource.getElementsByTagName("size").item(0).getTextContent();
                String sizeUnits = dataSource.getElementsByTagName("size").item(0).getAttributes().getNamedItem("units").getTextContent();
                String time = dataSource.getElementsByTagName("time").item(0).getTextContent();
                String timeUnits = dataSource.getElementsByTagName("time").item(0).getAttributes().getNamedItem("units").getTextContent();
                String filePath = dataSource.getElementsByTagName("FilePath").item(0).getTextContent();
                String filetype = dataSource.getElementsByTagName("FileType").item(0).getTextContent();
                String velocity = dataSource.getElementsByTagName("velocity").item(0).getTextContent();
                String velocityUnits = dataSource.getElementsByTagName("velocity").item(0).getAttributes().getNamedItem("units").getTextContent();


                windowSize = Integer.parseInt(size);
                windowVelocity = Integer.parseInt(velocity);
                System.out.println("Size : " + size + " " + sizeUnits);
                System.out.println("Clock Tick : " + time + " " + timeUnits);
                System.out.println("Velocity : " + velocity + " " + velocityUnits);

                if(timeUnits.contentEquals("seconds")){
                    windowClockTickInMillis = Integer.parseInt(time) * 1000;
                }
                else{
                    windowClockTickInMillis = Integer.parseInt(time);
                }
                System.out.println("File Path : " + filePath);

                csvPath = factsDir.concat(filePath.concat(filetype));
                csvFile = new File(csvPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        // Read the header line separately and split it into column names
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        String header = reader.readLine();
        byte[] bytes = header.getBytes(StandardCharsets.UTF_8);
        System.out.println("Bytes Read" + bytes.length);
        lastLineOffsetNew += bytes.length;
        System.out.println(header);
        String[] columnNames = header.split(",");
        reader.close();

        System.out.println("Clearing fact table");
        Statement statement = conn.createStatement();
        String deleteFactTableQuery = "DELETE FROM FactTable";
        statement.executeUpdate(deleteFactTableQuery);



        String insertQuery = "INSERT INTO FactTable (" + header + ") VALUES (" + String.join(",", Collections.nCopies(columnNames.length, "?")) + ")";
        PreparedStatement stmt = conn.prepareStatement(insertQuery);
        PreparedStatement deleteNRowsQuery = conn.prepareStatement("DELETE FROM FactTable LIMIT ?");


        while (true) {

            if(noOfRowsToBeDeleted > 0){
                deleteNRowsQuery.setInt(1, noOfRowsToBeDeleted);
                int rowsDeleted = deleteNRowsQuery.executeUpdate();
                System.out.println("FIRST " + rowsDeleted + " ROW DELETED FROM FACT TABLE");
            }
            else if(noOfRowsToBeDeleted <0){
                 statement = conn.createStatement();
                 deleteFactTableQuery = "DELETE FROM FactTable";
                int rowsDeleted = statement.executeUpdate(deleteFactTableQuery);
                System.out.println(rowsDeleted + " ALL ROWS DELETED FROM FACT TABLE");
            }

            System.out.println("CHECKING FOR NEW FACTS");
            FileReader fileReader = new FileReader(csvFile);
            fileReader.skip(lastLineOffsetNew);

            BufferedReader br = new BufferedReader(fileReader);
            String line;
            int noOfLinesReadInCurrentWindow = 0;
            int noOfSkippedLinesInCurrentWindow = 0;


            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty() || line.equals(header)) {
                    continue;
                }

                bytes = line.getBytes(StandardCharsets.UTF_8);
                lastLineOffsetNew += bytes.length +1;
                String[] values = line.split(",");

                if(!initialRead && ((windowSize-windowVelocity + noOfSkippedLinesInCurrentWindow) <0)){
                    noOfSkippedLinesInCurrentWindow++;
                    continue;
                }

                for (int i = 0; i < columnNames.length; i++) {
                    stmt.setString(i + 1, values[i]);
                }

                stmt.addBatch();
                noOfLinesReadInCurrentWindow++;
                System.out.println(stmt.toString());
                if(initialRead && noOfLinesReadInCurrentWindow >= windowSize){
                    break;
                }
                if(windowSize - windowVelocity < 0 && noOfLinesReadInCurrentWindow >= windowSize){
                    break;
                }

                 if (!initialRead && windowSize - windowVelocity > 0 && noOfLinesReadInCurrentWindow >= windowVelocity) {
                    break;
                }
                 if(!initialRead && windowSize-windowVelocity == 0 && noOfLinesReadInCurrentWindow == windowSize){
                     break;
                 }
            }
            initialRead=false;

            int[] updateCounts = stmt.executeBatch();

            System.out.println("INSERTED "+ updateCounts.length + " ROWS INTO FACT TABLE");
            if(noOfLinesReadInCurrentWindow < windowSize - windowVelocity){
                noOfRowsToBeDeleted = noOfLinesReadInCurrentWindow;
            }
            else if(windowSize == windowVelocity){
                noOfRowsToBeDeleted = windowSize;
            }
            else{
                noOfRowsToBeDeleted =  windowVelocity;
            }


            if(noOfSkippedLinesInCurrentWindow > 0){
                System.out.println("noOfSkippedLinesInCurrentWindow " + noOfSkippedLinesInCurrentWindow);
            }

            br.close();
            fileReader.close();

            // Wait for clockTick seconds before checking the file again
            Thread.sleep(windowClockTickInMillis);
        }
    }

}
