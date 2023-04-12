package org.example;

public class Driver {
    public static void main(String[] args) throws Exception {

        String dimensionsDirectory = "./dimensions/";
        String factsDirectory = "./facts/";
        String xmlFileName = "DMInstance.xml";
        DimensionalProcessing Dimproc = new DimensionalProcessing();
        Dimproc.CreateMetaDT();
        Dimproc.GenerateDTs(dimensionsDirectory, xmlFileName);
        FactTableProcessing factTableProcessing=new FactTableProcessing();
        factTableProcessing.GenerateFT(dimensionsDirectory,xmlFileName);
        QueryProcessing queryProcessing=new QueryProcessing();
        queryProcessing.GenerateSummary();
//        StreamProcessing streamProcessing = new StreamProcessing();
//        streamProcessing.start(factsDirectory,dimensionsDirectory,xmlFileName);
        /* Sliding Window Implementation... */
        NewStreamProcessing nsp = new NewStreamProcessing();
        nsp.startStreamService(factsDirectory, dimensionsDirectory, xmlFileName);

    }
}
