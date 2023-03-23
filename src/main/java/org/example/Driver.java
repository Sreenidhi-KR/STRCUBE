package org.example;

public class Driver {
    public static void main(String[] args){
        DimensionalProcessing Dimproc = new DimensionalProcessing();
        Dimproc.CreateMetaDT();
        String xmlFilePath = "/Users/boppanavenkatesh/Desktop/XML Practice/DM Instance.xml";
        Dimproc.GenerateDTs(xmlFilePath);
    }
}
