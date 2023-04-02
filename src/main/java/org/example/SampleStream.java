package org.example;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Timer;
import java.util.TimerTask;

public class SampleStream {
    String url = "jdbc:mysql://localhost:3306/DataModeling?sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'&jdbcCompliantTruncation=false&createDatabaseIfNotExist=true";
    String username = "root"; // replace with your username
    String password = "KVrsmck@21";

    String dimensionsDirectory = "dimensions/";
    String factsDirectory = "./facts/";
    String xmlFileName = "DMInstance.xml";

    public static int Offset = 0;
    public static int Size = 3;
    public static int Velocity = 5;
    public static String csvFile = "./facts/stream.csv";
    public static int TimeClick = 5000;

    public static void main(String[] args) throws IOException {

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
                    SlidingWindow();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 0, TimeClick);

    }
    public static void SlidingWindow() throws IOException {
        System.out.println("...............................");
        FileReader fileReader1 = new FileReader(csvFile);
        fileReader1.skip(Offset+1);
        FileReader fileReader2 = new FileReader(csvFile);
        fileReader2.skip(Offset+1);
        BufferedReader R = new BufferedReader(fileReader1);
        BufferedReader S = new BufferedReader(fileReader2);
        int temp = Velocity, window = Size;
        while (window > 0){
            String csvLine = S.readLine();
            byte[] lineBytes = csvLine.getBytes(StandardCharsets.UTF_8);
            System.out.println(csvLine);
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
