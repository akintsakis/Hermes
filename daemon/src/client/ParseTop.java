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
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class ParseTop {

    static Double pidStatInterval = 1.0;
    static long topInterval = 1;

    public static HashMap<String, String> getProcesses(String filename) throws IOException {
        HashMap<String, String> processes = new HashMap<String, String>();
        //List<String> lines = Files.readAllLines(new File(filename).toPath(), StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
        //String t = lines.get(0);
        String line;
        while ((line = br.readLine()) != null) {
            String[] tmp = line.split(" ");
            for (int i = 0; i < tmp.length; i++) {
                if (!processes.containsKey(tmp[i])) {
                    processes.put(tmp[i], "");
                }
            }
        }
        return processes;
    }

    public static ArrayList<Double> removePreceedingZeroes(ArrayList<Double> usagePerInterval, double runtime) {
        int numvalues = ((int) runtime + 1000) / 1000;
        ArrayList<Double> newUsagePerInterval = new ArrayList<Double>();
        if (usagePerInterval.size() > numvalues) {
            for (int i = 0; i < numvalues; i++) {
                newUsagePerInterval.add(usagePerInterval.get(usagePerInterval.size() - numvalues + i));
            }
            return newUsagePerInterval;
        }
        return usagePerInterval;
    }

    public static void parseDiskIOFile(JobResponse jobResponse, String filename, HashMap<String, String> processes, long startedAtUnixTimestamp, double runtime) throws IOException {
        //List<String> lines = Files.readAllLines(new File(filename).toPath(), StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
        double totalReads = 0.0;
        double totalWrites = 0.0;

        String line;
        int i=0;
        while ((line = br.readLine()) != null) {

            //String line = lines.get(i);
            if (i > 0) {
                line = line.replaceAll(" +", " ");
                if (line.length() > 10 && !line.contains("UID PID")) {
                    //System.out.println(line);
                    String[] tmp = line.split(" ");
                    if (tmp.length > 5 && (Long.valueOf(tmp[1]) > startedAtUnixTimestamp)) {
                        String pid = tmp[3];
                        if (processes.containsKey(pid)) {
                            //System.out.println(line);
                            totalReads = totalReads + Double.valueOf(tmp[4]) * pidStatInterval;
                            totalWrites = totalReads + Double.valueOf(tmp[5]) * pidStatInterval;
                        }
                    }
                }
            }
            i++;
        }
        jobResponse.totalReads= String.format("%.1f", totalReads);
        jobResponse.totalWrites=String.format("%.1f", totalWrites);
        

    }

    public static void parseTopFile(JobResponse jobResponse, String filename, HashMap<String, String> processes, long startedAtSecond, double runtime) throws IOException {
        //List<String> lines = Files.readAllLines(new File(filename).toPath(), StandardCharsets.UTF_8);
        ArrayList<Double> cpuUsagePerInterval = new ArrayList<Double>();
        ArrayList<Double> memUsagePerInterval = new ArrayList<Double>();

        double cpuUsage = 0.0;
        double memUsage = 0.0;
        int i = 0;

        startedAtSecond = startedAtSecond / 1000;
        long afterLines = (startedAtSecond / topInterval) - 5;
        long currentLine = -1;
        boolean start = false;
        String line;
        BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
        while ((line = br.readLine()) != null) {// System.out.println(i);
            //String line = lines.get(i);
            //System.out.println(line);

            if (!start) {
                if (line.startsWith("top -")) {
                    currentLine++;
                    if (currentLine > afterLines) {
                        //System.out.println("set true");
                        start = true;
                    }
                }
                i++;
            } else if (start) {
                if (line.startsWith("top -")) {
                    //System.out.println(line);
                    i = i + 7;
                    cpuUsagePerInterval.add(cpuUsage);
                    memUsagePerInterval.add(memUsage);
                    cpuUsage = 0.0;
                    memUsage = 0.0;

                } else {
                    line = line.replaceAll(" +", " ");
                    if (line.startsWith(" ")) {
                        line = line.substring(1, line.length());
                    }
                    String[] tmp = line.split(" ");
                    if (tmp.length > 10) {
                        String currentPid = tmp[0];
                        if (processes.containsKey(currentPid)) {
                            cpuUsage = cpuUsage + Double.valueOf(tmp[8]);
                            memUsage = memUsage + Double.valueOf(tmp[9]);
                        }
                    }
                    i++;
                }
            }
        }

        cpuUsagePerInterval = removePreceedingZeroes(cpuUsagePerInterval, runtime);
        memUsagePerInterval = removePreceedingZeroes(memUsagePerInterval, runtime);
        Collections.max(cpuUsagePerInterval);

        StringBuilder cpuUsagePerIntevalString = new StringBuilder();
        StringBuilder memUsagePerIntevalString = new StringBuilder();

        double averageCpuUsage = 0.0;
        double averageMemUsage = 0.0;
        for (int j = 0; j < cpuUsagePerInterval.size(); j++) {
            cpuUsagePerIntevalString.append(String.format("%.1f", cpuUsagePerInterval.get(j))).append(" ");
            memUsagePerIntevalString.append(String.format("%.1f", memUsagePerInterval.get(j))).append(" ");
            averageCpuUsage = averageCpuUsage + cpuUsagePerInterval.get(j);
            averageMemUsage = averageMemUsage + memUsagePerInterval.get(j);
        }
        averageCpuUsage = averageCpuUsage / (double) cpuUsagePerInterval.size();
        averageMemUsage = averageMemUsage / (double) cpuUsagePerInterval.size();

        jobResponse.topIntervalTimeInSeconds= String.valueOf(topInterval);
        jobResponse.averageCpuUsage=String.valueOf(averageCpuUsage);
        jobResponse.averageMemUsage=String.valueOf(averageMemUsage);

        jobResponse.maxCpuUsage=String.valueOf(String.format("%.1f", Collections.max(cpuUsagePerInterval)));
        jobResponse.maxMemoryUsage=String.valueOf(String.format("%.1f", Collections.max(memUsagePerInterval)));
    }

    public static void logstoJson(JobResponse jobResponse, String processFile, String topFile, String diskFile, long startedAtSecond, long startedAtUnixTimestamp, double runtime) throws IOException {
        HashMap<String, String> processes = getProcesses(processFile);
        parseTopFile(jobResponse, topFile, processes, startedAtSecond, runtime);
        parseDiskIOFile(jobResponse, diskFile, processes, startedAtUnixTimestamp, runtime);

    }

}
