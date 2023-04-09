package org.example;

public class Driver {
    public static void main(String[] args) throws Exception {
//        DimensionalProcessing Dimproc = new DimensionalProcessing();
//        Dimproc.CreateMetaDT();
        String dimensionsDirectory = "./dimensions/";
        String factsDirectory = "./facts/";
        String xmlFileName = "DMInstance.xml";
//        Dimproc.GenerateDTs(dimensionsDirectory, xmlFileName);
//        FactTableProcessing factTableProcessing=new FactTableProcessing();
//        factTableProcessing.GenerateFT(dimensionsDirectory,xmlFileName);
//        StreamProcessing streamProcessing = new StreamProcessing();
//        streamProcessing.start(factsDirectory,dimensionsDirectory,xmlFileName);
        /* Sliding Window Implementation... */
        NewStreamProcessing nsp = new NewStreamProcessing();
        nsp.startStreamService(factsDirectory, dimensionsDirectory, xmlFileName);
//        QueryProcessing queryProcessing=new QueryProcessing();
//        queryProcessing.GenerateSummary();
    }
}
