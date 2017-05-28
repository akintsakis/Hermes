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

public class DataFileEvaluationFunctions {

    public static final String fasta = ".fasta";
    public static final String blastp = ".blastp";
    public static final String blastproteindb = ".blastdbprotein";
    public static final String blastproteindbfolder = "makeBlastProteinDB";

    public static String selector(String filename, JobRequest jobRequest) throws IOException {
        //String type;
        String sizeOfFile = "1";
        if (filename.contains(fasta)) {
            sizeOfFile = countBasesFasta(filename);
        } else if (filename.contains(blastp)) {
            sizeOfFile = countLines(filename);
        } else if (filename.contains(blastproteindb) || filename.contains(blastproteindbfolder)) {
            sizeOfFile = inheritCustomSizeOfInput1(filename, jobRequest);
        }
        return sizeOfFile;
    }

    public static String inheritCustomSizeOfInput1(String file, JobRequest jobRequest) {
        String inputsCustom = jobRequest.inputsRealFileSizesCustom;
        return (inputsCustom.split(" ")[0]);
    }

    public static String countLines(String file) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(file)));
            int lines = 0;
            while (reader.readLine() != null) {
                lines++;
            }
            reader.close();
            return String.valueOf(lines);
        } catch (IOException e) {
            return "0";
        }
    }

    public static String countBasesFasta(String path) {
        try {
            File f = new File(path);

            BufferedReader r1 = new BufferedReader(new FileReader(f.getAbsolutePath()));

            String line;
            int totalBases = 0;

            while ((line = r1.readLine()) != null) {
                if (!line.contains(">")) {
                    totalBases = totalBases + line.length();
                }
            }
            return String.valueOf(totalBases);
        } catch (IOException e) {
            return "0";
        }

    }

}
