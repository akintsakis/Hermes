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

/**
 *
 * @author thanos
 */
public class Parsers {

    final static String runtimeDatasetIdentifier = "_runtime";
    final static String failureDatasetIdentifier = "_failure";
    static Gson gson = new Gson();

    public static void createDatasets(String folderPath, String componentName, String modelOutPath) throws IOException {

        File[] files = (new File(folderPath)).listFiles();

        BufferedWriter runtimeWr = new BufferedWriter(new FileWriter(new File(modelOutPath + "/" + componentName + runtimeDatasetIdentifier)));
        BufferedWriter failureWr = new BufferedWriter(new FileWriter(new File(modelOutPath + "/" + componentName + failureDatasetIdentifier)));

        for (int i = 0; i < files.length; i++) {
            JsonReader reader = new JsonReader(new FileReader(files[i]));
            JobResponse jobResponse = gson.fromJson(reader, JobResponse.class);

            StringBuilder line = new StringBuilder();

            line.append(jobResponse.jobRequest.cpuMultiThreadedBenchmark).append(",");
            line.append(jobResponse.jobRequest.cpuSingleThreadedBenchmark).append(",");
            line.append(jobResponse.jobRequest.inputsRealFileSizesCustom.replace(" ", "")).append(",");
            line.append(jobResponse.jobRequest.inputsRealFileSizesInB.replace(" ", "")).append(",");
            line.append(jobResponse.jobRequest.totalSystemCPUs).append(",");
            line.append(jobResponse.jobRequest.timeout).append(",");
            line.append(jobResponse.jobRequest.systemRAMSizeInMB).append(",");
            line.append(jobResponse.jobRequest.threadsUsedByComponent).append(",");

            

            String success = "0";
            if (jobResponse.success) {
                success = "1";
            }
            String failureLine = line.toString()+success;
            String runtimeLine = line.toString()+jobResponse.runtime;
            line.append(success);

            failureWr.write(failureLine);
            failureWr.newLine();
//           
            runtimeWr.write(runtimeLine);
            runtimeWr.newLine();

        }

        runtimeWr.close();
        failureWr.close();

    }

}
