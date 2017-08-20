/*
 * Copyright 2016 Athanassios Kintsakis.
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
 *
 * Author: Athanassios Kintsakis
 * contact: akintsakis@issel.ee.auth.gr athanassios.kintsakis@gmail.com
 */
package client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataFileEvaluationFunctions {

    public static final String fasta = ".fasta";
    public static final String blastp = ".blastp";
    public static final String blastproteindb = ".blastdbprotein";
    public static final String fileSizeKey = "sizeInBytes";

    public static void selector(String filename, Map<String, String> metrics, boolean advancedEval, JobRequest jobRequest) throws IOException {
        if (advancedEval) {
            if (filename.endsWith(fasta)) {
                metrics.putAll(countBasesSeqsFasta(filename));
            } else if (filename.contains(blastp)) {
                metrics.put("lines", countLines(filename));
            } else if (filename.contains(blastproteindb)) {
                metrics.putAll(inheritInputMetrics(jobRequest));
                //sizeOfFile = inheritCustomSizeOfInput1(filename);
            } else {
                metrics.put("lines", countLines(filename));
            }
        }
        metrics.put(fileSizeKey, getFileSizeInBytes(filename));
    }

    public static Map<String, String> inheritInputMetrics(JobRequest jobRequest) {
        Map<String, String> metrics = new HashMap<String, String>();

        int count =0;
        for (Integer i : jobRequest.inputDataFileIds) {
            String s = String.valueOf(i);
            for (Map.Entry<String, String> entry : jobRequest.jobInputFileMetrics.get(s).entrySet()) {
                
                metrics.put("input_" + count +"_"+  entry.getKey(), entry.getValue());
            }
            count++;
        }
        return metrics;
    }

    public static String getFileSizeInBytes(String file) {
        File currentOutput = new File(file);
        String fileSize = "0";

        if (!currentOutput.exists()) {
            Path folder = currentOutput.getParentFile().toPath();
            try {
                fileSize = String.valueOf(Files.walk(folder)
                        .filter(p -> p.toFile().isFile())
                        .mapToLong(p -> p.toFile().length())
                        .sum());
            } catch (IOException ex) {
                Logger.getLogger(DataFileEvaluationFunctions.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            fileSize = String.valueOf(currentOutput.length());
        }
        return fileSize;
    }

    public static String countLines(String file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(file)));
        int lines = 0;
        while (reader.readLine() != null) {
            lines++;
        }
        reader.close();
        return String.valueOf(lines);
    }

    public static Map<String, String> countBasesSeqsFasta(String path) throws FileNotFoundException, IOException {
        Map<String, String> metrics = new HashMap<String, String>();
        File f = new File(path);
        BufferedReader r1 = new BufferedReader(new FileReader(f.getAbsolutePath()));
        String line;
        long totalBases = 0;
        long totalSeqs = 0;
        long totalLines = 0;
        while ((line = r1.readLine()) != null) {
            if (!line.contains(">")) {
                totalBases = totalBases + line.length();
            } else if (line.startsWith(">")) {
                totalSeqs++;
            }
            totalLines++;
        }
        metrics.put("sequences", String.valueOf(totalSeqs));
        metrics.put("bases", String.valueOf(totalBases));
        metrics.put("lines", String.valueOf(totalLines));

        return metrics;
    }

}
