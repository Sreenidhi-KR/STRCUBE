package org.example;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Timer;
import java.util.TimerTask;

public class SampleStream {
    public static String url = "jdbc:mysql://localhost:3306/DataModeling?sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'&jdbcCompliantTruncation=false&createDatabaseIfNotExist=true";
    public static String username = "root"; // replace with your username
    public static String password = "KVrsmck@21";
    public static int Offset = 0;

    public static int Size = 5;
    public static int Velocity = 3;
    public static String csvFile = "./facts/stream.csv";

    public static  String[] columnTypes = {"VARCHAR(255)","VARCHAR(255)","VARCHAR(255)","VARCHAR(255)","DECIMAL","DECIMAL","DECIMAL"};
    public static int TimeClick = 5000;


    public static void main(String[] args) throws IOException, SQLException {
        // Setup Connection
        Connection conn = DriverManager.getConnection(url,username,password);

        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        String header = reader.readLine();
        byte[] bytes = header.getBytes(StandardCharsets.UTF_8);
        System.out.println("Bytes Read" + bytes.length);
        Offset += bytes.length;
        System.out.println(header);
        String[] columnNames = header.split(",");
        reader.close();

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    SlidingWindow(conn, header);
                } catch (IOException | SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 0, TimeClick);

    }
    public static void SlidingWindow(Connection conn, String header) throws IOException, SQLException {
        System.out.println("...............................");
        System.out.println("[Clearing fact table...]");
        Statement statement = conn.createStatement();
        String truncateFactTableQuery = "TRUNCATE TABLE FactTable";
        statement.executeUpdate(truncateFactTableQuery);

        FileReader fileReader1 = new FileReader(csvFile);
        fileReader1.skip(Offset+1);
        FileReader fileReader2 = new FileReader(csvFile);
        fileReader2.skip(Offset+1);
        BufferedReader R = new BufferedReader(fileReader1);
        BufferedReader S = new BufferedReader(fileReader2);
        int temp = Velocity, window = Size;
        while (window > 0){
            String csvLine = S.readLine();
            String[] values = csvLine.split(",");
            String insertSql = "INSERT INTO FactTable (" + header;
            insertSql += ") VALUES (";
            for (int i = 0; i < values.length; i++) {
                if (columnTypes[i].equals("VARCHAR(255)")) {
                    insertSql += "'" + values[i] + "'";
                } else {
                    insertSql += values[i];
                }
                if (i < values.length - 1) {
                    insertSql += ", ";
                }
            }
            insertSql += ")";
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(insertSql);
            System.out.println(insertSql+" Executed Successfully...");
            --window;
        }
        while (!(S.readLine() == null) && temp > 0){
            --temp;
        }
        int avail = Velocity - temp;

        for(int i = 0 ; i < avail ; i++){
            String csvLine = R.readLine();
            byte[] lineBytes = csvLine.getBytes(StandardCharsets.UTF_8);
            Offset += lineBytes.length + 1;
        }
    }
}
