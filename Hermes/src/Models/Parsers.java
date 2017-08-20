/*
 * Copyright 2017 thanos.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package Models;

import Hermes.JobResponse;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.FileReader;
import java.util.Map;

/**
 *
 * @author thanos
 */
public class Parsers {

    final static String runtimeDatasetIdentifier = "_runtime";
    final static String failureDatasetIdentifier = "_failure";
    final static String outputFileSizeCustomIdentifeir = "_outputfilesizecustom";
    static Gson gson = new Gson();

    public static void loadPerFile() {

    }

    public static void createDatasets(String folderPath, String componentName, String modelOutPath) throws IOException {

        File[] files = (new File(folderPath)).listFiles();

        BufferedWriter runtimeWr = new BufferedWriter(new FileWriter(new File(modelOutPath + "/" + componentName + runtimeDatasetIdentifier)));
        BufferedWriter failureWr = new BufferedWriter(new FileWriter(new File(modelOutPath + "/" + componentName + failureDatasetIdentifier)));
        BufferedWriter outputFileCustomWr = new BufferedWriter(new FileWriter(new File(modelOutPath + "/" + componentName + outputFileSizeCustomIdentifeir)));

        long failed = 0;
        for (int i = 0; i < files.length; i++) {
            JsonReader reader = new JsonReader(new FileReader(files[i]));
            JobResponse jobResponse = gson.fromJson(reader, JobResponse.class);

            StringBuilder line = new StringBuilder();

            line.append(jobResponse.jobRequest.cpuMultiThreadedBenchmark).append(",");
//            line.append(jobResponse.jobRequest.cpuSingleThreadedBenchmark).append(",");
//            line.append(jobResponse.jobRequest.totalSystemCPUs).append(",");
            //line.append(jobResponse.jobRequest.timeout).append(",");
//            line.append(jobResponse.jobRequest.systemRAMSizeInMB).append(",");
//            line.append(jobResponse.jobRequest.threadsUsedByComponent).append(",");

            for (int j = 0; j < jobResponse.jobRequest.inputDataFileIds.size(); j++) {

                Map<String, String> m = jobResponse.jobRequest.jobInputFileMetrics.get(String.valueOf(jobResponse.jobRequest.inputDataFileIds.get(j)));
                if (j == 1) {
                    line.append(m.get("input_0_bases")).append(",");
                    //line.append(m.get("input_0_sizeInBytes")).append(",");
                    //line.append(m.get("sizeInBytes")).append(",");
                    line.append(m.get("input_0_sequences")).append(",");
                    //line.append(m.get("input_0_lines")).append(",");

                } else if (j == 0) {
                    line.append(m.get("bases")).append(",");
                    //line.append(m.get("sizeInBytes")).append(",");
                    line.append(m.get("sequences")).append(",");
                    //line.append(m.get("lines")).append(",");
                }
            }

            //line.append(jobResponse.jobRequest.jobInputFileMetrics).append(",");
            //outMap
            String success = "0";
            if (jobResponse.success) {
                success = "1";
            } else {
                failed++;
            }
            String failureLine = line.toString() + success;

            //String outputFileSizeLine = line.toString() + jobResponse.outputDataFileSizesCustom.trim();
            //line.append(success);
            failureWr.write(failureLine);
            failureWr.newLine();
//          
            if (jobResponse.success) {
                Map<String, String> outFileMetrics = jobResponse.jobOutputFileMetrics.get(String.valueOf(jobResponse.jobRequest.outputDataFileIds.get(0)));
                System.out.println(outFileMetrics.size());
                String met = outFileMetrics.get("lines");
                String runtimeLine = line.toString() + String.valueOf(jobResponse.runtime);
                runtimeWr.write(runtimeLine);
                runtimeWr.newLine();

                //outputFileCustomWr.write(outputFileSizeLine);
                //outputFileCustomWr.newLine();
            }

        }
        System.out.println("Failed: " + failed);
        runtimeWr.close();
        failureWr.close();
        outputFileCustomWr.close();

    }

}
