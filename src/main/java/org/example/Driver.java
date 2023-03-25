package org.example;

public class Driver {
    public static void main(String[] args){
        DimensionalProcessing Dimproc = new DimensionalProcessing();
        Dimproc.CreateMetaDT();
        String workingDir = "./dimensions";
        String xmlFileName = "/DM Instance.xml";
        Dimproc.GenerateDTs(workingDir, xmlFileName);
        FactTableProcessing factTableProcessing=new FactTableProcessing();
        factTableProcessing.GenerateFT(workingDir,xmlFileName);
    }
}
