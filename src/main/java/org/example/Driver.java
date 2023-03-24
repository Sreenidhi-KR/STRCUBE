package org.example;

public class Driver {
    public static void main(String[] args){
        DimensionalProcessing Dimproc = new DimensionalProcessing();
        Dimproc.CreateMetaDT();
        String workingDir = "/Users/boppanavenkatesh/Desktop/dimensions";
        String xmlFileName = "/DM Instance.xml";
        Dimproc.GenerateDTs(workingDir, xmlFileName);
    }
}
