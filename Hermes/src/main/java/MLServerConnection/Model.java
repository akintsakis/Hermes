package MLServerConnection;


import Hermes.Configuration;
import Hermes.JobRequest;
import Hermes.JobResponse;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Model {

    public static Map<String, Model> models = new HashMap<String, Model>(); //key componentname_attribute

    public final String componentName;
    public final String attribute;
    public final String id;
    public final String predictionType;
    public final String dataSetPath;

    Model(String componentName, String attribute, String predictionType, String dataSetPath) {
        this.componentName = componentName;
        this.attribute = attribute;
        this.predictionType = predictionType;
        this.dataSetPath = dataSetPath;
        this.id=this.componentName+"_"+attribute;
    }
    static Gson gson = new Gson();

    public static ArrayList<String> getOutputFileMetrics(JobResponse jobResponse) {
        ArrayList<String> outputMetrics = new ArrayList<String>();
        return outputMetrics;
    }

    public static ArrayList<String> getInputFileMetrics(JobRequest jobRequest) {
        ArrayList<String> inputFileMetrics = new ArrayList<String>();
        return inputFileMetrics;
    }

    public static void createComponentModels(String componentName) throws IOException {

        System.out.println(componentName);
        File[] files = (new File(Configuration.globalConfig.locationOfHermesComponentExecutionLogs + componentName)).listFiles();

        BufferedWriter runtimeWr = new BufferedWriter(new FileWriter(new File(Configuration.globalConfig.locationOfComponentModelDatasets + componentName + "_runtime")));
        BufferedWriter failureWr = new BufferedWriter(new FileWriter(new File(Configuration.globalConfig.locationOfComponentModelDatasets + componentName + "_failure")));

        Model modelRuntime = new Model(componentName, "runtime", "regression", Configuration.globalConfig.locationOfComponentModelDatasets + componentName + "_runtime");
        models.put(modelRuntime.id, modelRuntime);

        Model modelFailure = new Model(componentName, "failure", "classification", Configuration.globalConfig.locationOfComponentModelDatasets + componentName + "_failure");
        models.put(modelFailure.id, modelFailure);

        Map<String, BufferedWriter> outputSizeDatasets = new HashMap<String, BufferedWriter>();

        //BufferedWriter outputFileCustomWr = new BufferedWriter(new FileWriter(new File(modelOutPath + "/" + componentName + outputFileSizeCustomIdentifeir)));

        long failed = 0;

        ArrayList<String[]> orderedInputMetricNames = new ArrayList<String[]>();
        ArrayList<String[]> orderedOutputMetricNames = new ArrayList<String[]>();
        long maxVariableFeatures = 0;
        boolean once = true;
        for (int i = 0; i < files.length; i++) {
            long variableFeatures = 0;
            JsonReader reader = new JsonReader(new FileReader(files[i]));
            JobResponse jobResponse = gson.fromJson(reader, JobResponse.class);

            if (jobResponse.success) {
                for (int j = 0; j < jobResponse.jobRequest.inputDataFileIds.size(); j++) {
                    Map<String, String> m = jobResponse.jobRequest.jobInputFileMetrics.get(String.valueOf(jobResponse.jobRequest.inputDataFileIds.get(j)));
                    if (once) {
                        orderedInputMetricNames.add(m.keySet().toArray(new String[0]));
                    }
                    variableFeatures = variableFeatures + m.keySet().size();

                }
                if (variableFeatures > maxVariableFeatures) {
                    maxVariableFeatures = variableFeatures;
                }

                for (int j = 0; j < jobResponse.jobRequest.outputDataFileIds.size(); j++) {
                    Map<String, String> m = jobResponse.jobOutputFileMetrics.get(String.valueOf(jobResponse.jobRequest.outputDataFileIds.get(j)));
                    if (once) {
                        orderedOutputMetricNames.add(m.keySet().toArray(new String[0]));
                    }

                }
                once = false;
            }
        }

        for (int i = 0; i < files.length; i++) {
            //System.out.println(files[i].getName());
            JsonReader reader = new JsonReader(new FileReader(files[i]));
            JobResponse jobResponse = gson.fromJson(reader, JobResponse.class);
            //MLServerConnectionUtils.submitJsonObject(gson.toJson(jobResponse));
            StringBuilder features = new StringBuilder();

            features.append(jobResponse.jobRequest.cpuMultiThreadedBenchmark).append(",");
            features.append(jobResponse.jobRequest.cpuSingleThreadedBenchmark).append(",");
            features.append(jobResponse.jobRequest.totalSystemCPUs).append(",");
            features.append(jobResponse.jobRequest.timeout).append(",");
            features.append(jobResponse.jobRequest.systemRAMSizeInMB).append(",");
            features.append(jobResponse.jobRequest.threadsUsedByComponent).append(",");

            long variableFeatures = 0;
            for (int j = 0; j < jobResponse.jobRequest.inputDataFileIds.size(); j++) {
                Map<String, String> m = jobResponse.jobRequest.jobInputFileMetrics.get(String.valueOf(jobResponse.jobRequest.inputDataFileIds.get(j)));
                String[] keys = orderedInputMetricNames.get(j);
                for (String key : keys) {
                    features.append(m.get(key)).append(",");
                    variableFeatures++;
                }
            }
            if (variableFeatures < maxVariableFeatures) {
                for (int j = 0; j < (maxVariableFeatures - variableFeatures); j++) {
                    features.append("0").append(",");
                }
            }

            String success = "1";
            if (jobResponse.success) {
                success = "0";
            } else {
                failed++;
            }
            String failureLine = features.toString() + success;
            failureWr.write(failureLine);
            failureWr.newLine();

            if (jobResponse.success) {
                String runtimeLine = features.toString() + String.valueOf(jobResponse.runtime);
                runtimeWr.write(runtimeLine);
                runtimeWr.newLine();


                for (int j = 0; j < orderedOutputMetricNames.size(); j++) {
                    String[] keys = orderedOutputMetricNames.get(j);
                    for (String key : keys) {
                        String fileName = "output_" + j + "_" + key;

                        if (!outputSizeDatasets.containsKey(fileName)) {
                            BufferedWriter outputFileCustomWr = new BufferedWriter(new FileWriter(new File(Configuration.globalConfig.locationOfComponentModelDatasets + componentName + "_" + fileName)));
                            Model model = new Model(componentName, fileName, "regression", Configuration.globalConfig.locationOfComponentModelDatasets + componentName + "_" + fileName);
                            models.put(model.id,model);
                            outputSizeDatasets.put(fileName, outputFileCustomWr);
                        }
                        BufferedWriter outputFileCustomWr = outputSizeDatasets.get(fileName);
                        Map<String, String> m = jobResponse.jobOutputFileMetrics.get(String.valueOf(jobResponse.jobRequest.outputDataFileIds.get(j)));
                        String targetValue = m.get(key);

                        outputFileCustomWr.write(features + targetValue);
                        outputFileCustomWr.newLine();
                    }
                }

            }

        }

        runtimeWr.close();
        failureWr.close();

        for (Map.Entry<String, BufferedWriter> entry : outputSizeDatasets.entrySet()) {
            entry.getValue().close();
        }

    }


}
