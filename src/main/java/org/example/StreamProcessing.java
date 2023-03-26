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
import java.sql.*;
import java.util.Collections;

public class StreamProcessing {

    public static void start(String factsDir ,String dimensionDir, String xmlFileName) throws InterruptedException, IOException, ClassNotFoundException, SQLException {
        int windowSize = 0;
        int windowIntervalInMillis = 0;
        String csvPath;
        File csvFile = null;

        String url = "jdbc:mysql://localhost:3306/DataModeling?sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'&jdbcCompliantTruncation=false&createDatabaseIfNotExist=true";
        String username = "sreenidhi"; // replace with your username
        String password = "apple101";
        
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

                windowSize = Integer.parseInt(size);
                System.out.println("Size : " + size + " " + sizeUnits);
                System.out.println("Time : " + time + " " + timeUnits);
                if(timeUnits.contentEquals("seconds")){
                    windowIntervalInMillis = Integer.parseInt(time) * 1000;
                }
                else{
                    windowIntervalInMillis = Integer.parseInt(time);
                }
                System.out.println("File Path : " + filePath);

                csvPath = factsDir.concat(filePath.concat(filetype));
                csvFile = new File(csvPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

 

        long lastLineOffset = 0;

        // Read the header line separately and split it into column names
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        String header = reader.readLine();
        System.out.println(header);
        String[] columnNames = header.split(",");
        reader.close();

        // Set up the database connection
        Connection conn = DriverManager.getConnection(url,username,password);

        // Prepare the insert statement
        String insertQuery = "INSERT INTO FactTable (" + header + ") VALUES (" + String.join(",", Collections.nCopies(columnNames.length, "?")) + ")"; PreparedStatement stmt = conn.prepareStatement(insertQuery);
        System.out.println(insertQuery);

        while (true) {
            // Open the file and skip to the last line that was printed
            System.out.println("DELETING FACT TABLE");
            Statement statement = conn.createStatement();

            // Execute the SQL statement to delete all rows in the fact table
            String deleteFactTableQuery = "DELETE FROM FactTable";
            statement.executeUpdate(deleteFactTableQuery);
            System.out.println("CHECKING FOR NEW FACTS");
            FileReader fileReader = new FileReader(csvFile);
            fileReader.skip(lastLineOffset);

            // Insert any new lines that have been added to the file
            BufferedReader br = new BufferedReader(fileReader);
            String line;
            int counter = 0;
            while ((line = br.readLine()) != null) {
                // Skip the first line (header)
                if (line.trim().isEmpty() || line.equals(header)) {
                    continue;
                }
                String[] values = line.split(",");
                for (int i = 0; i < columnNames.length; i++) {
                    stmt.setString(i + 1, values[i]);
                }

                stmt.addBatch();
                counter++;
                System.out.println(stmt.toString());
                if (counter >= windowSize) {
                    break;
                }
            }
            int[] updateCounts = stmt.executeBatch();
            System.out.println("INSERTED "+ updateCounts.length + " ROWS INTO FACTTABLE");
            // Remember the byte offset of the last line that was printed
            lastLineOffset = csvFile.length();

            // Close the file
            br.close();
            fileReader.close();

            // Wait for 5 seconds before checking the file again
            Thread.sleep(windowIntervalInMillis);
        }
    }

}
