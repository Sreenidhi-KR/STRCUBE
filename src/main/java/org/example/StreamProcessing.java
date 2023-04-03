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
        boolean initialRead = true;
        boolean earlySleep = false;


        String url = "jdbc:mysql://localhost:3306/DataModeling?sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'&jdbcCompliantTruncation=false&createDatabaseIfNotExist=true";
        String username = "sreenidhi"; // replace with your username
        String password = "apple101";
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



        String insertQuery = "INSERT INTO FactTable (" + header + ") VALUES (" + String.join(",", Collections.nCopies(columnNames.length, "?")) + ")"; PreparedStatement insertStatement = conn.prepareStatement(insertQuery);
        PreparedStatement deleteNRowsQuery = conn.prepareStatement("DELETE FROM FactTable LIMIT ?");


        while (true) {

            System.out.println("CHECKING FOR NEW FACTS");
            FileReader fileReader = new FileReader(csvFile);
            fileReader.skip(lastLineOffsetNew);

            BufferedReader br = new BufferedReader(fileReader);
            String line;
            int noOfLinesReadInCurrentWindow = 0;
            int noOfSkippedLinesInCurrentWindow = 0;


            while (true) {
                line = br.readLine();
                if(line == null){
                    earlySleep = true;
                    //Do any custom logic here like to recheck every 't' min
                    break;
                }
                if (line.trim().isEmpty() || line.equals(header)) {
                    continue;
                }

                bytes = line.getBytes(StandardCharsets.UTF_8);
                lastLineOffsetNew += bytes.length +1;
                String[] values = line.split(",");

                //Checking if we need to skip rows
                if(!initialRead && ((windowSize-windowVelocity + noOfSkippedLinesInCurrentWindow) <0)){
                    noOfSkippedLinesInCurrentWindow++;
                    continue;
                }

                for (int i = 0; i < columnNames.length; i++) {
                    insertStatement.setString(i + 1, values[i]);
                }

                insertStatement.addBatch();
                noOfLinesReadInCurrentWindow++;
                System.out.println(insertStatement.toString());

                if(initialRead){
                    if( noOfLinesReadInCurrentWindow  == windowSize){
                        break;
                    }
                }
                else {
                    if (windowSize  <= windowVelocity && noOfLinesReadInCurrentWindow == windowSize) {
                        break;
                    }
                    if (windowSize > windowVelocity && noOfLinesReadInCurrentWindow == windowVelocity) {
                        break;
                    }
                }
            }


            deleteNRowsQuery.setInt(1, noOfLinesReadInCurrentWindow);
            int rowsDeleted = deleteNRowsQuery.executeUpdate();
            System.out.println("FIRST " + rowsDeleted + " ROW DELETED FROM FACT TABLE");


            int[] updateCounts = insertStatement.executeBatch();
            System.out.println("INSERTED "+ updateCounts.length + " ROWS INTO FACT TABLE");


            if(noOfSkippedLinesInCurrentWindow > 0){
                System.out.println("noOfSkippedLinesInCurrentWindow " + noOfSkippedLinesInCurrentWindow);
            }



            br.close();
            fileReader.close();
            initialRead=false;

            if(earlySleep){
                initialRead = true;
            }
            earlySleep = false;

            // Wait for clockTick seconds before checking the file again
            Thread.sleep(windowClockTickInMillis);
        }
    }

}
