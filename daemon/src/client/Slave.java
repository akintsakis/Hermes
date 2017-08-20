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

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Slave extends Thread {

    int id;
    static String baseDir;
    String topLog;
    String diskLog;

    static String componentRuntimeLogs;
    static String wrapper;
    static ServerSocket jobSocket;
    static String masterAddress;
    static String masterJobPort;
    //JSONObject incomingJSON
    JobRequest jobRequest;
    JobResponse jobResponse;
    BufferedWriter wr1;

    public long startedAtSecond;
    public long startedAtUnixTimestamp;

    String processLog = "";
    String mscError = "";
    String errorLog = "";
    String screenName;
    //long timeOut;

    Gson gson = new Gson();

    public void buildProcessLogsToJson(double runTime) {
        //JSONObject entry = new JSONObject();
        try {
            ParseTop.logstoJson(jobResponse, processLog, topLog, diskLog, startedAtSecond, startedAtUnixTimestamp, runTime);
        } catch (IOException ex) {
            Logger.getLogger(Slave.class.getName()).log(Level.SEVERE, null, ex);
        }

        //return entry;
    }

    public static String waitForCommand() {
        try {
            Socket server = jobSocket.accept();
            String sender = server.getRemoteSocketAddress().toString();
            System.out.println(sender);
            DataInputStream in = new DataInputStream(server.getInputStream());
            String message = in.readUTF();

            server.close();
            return message;
        } catch (SocketTimeoutException s) {
            System.out.println("Socket timed out!");
            return "timeout exception";

        } catch (IOException e) {
            e.printStackTrace();

        }
        return "Error";
    }

    public String executeBashScript(String[] cmd) {
        Process p;
        String exitValueString = "originalNone";
        StreamGobbler errorGobbler;
        StreamGobbler outputGobbler;
        String error = "originalNone";
        int maxStrangeRetries = 5;
        int maxFileTransferRetries = 8;
        int exitValue = 1;
        if(jobRequest.jobIsFileTransfer) {
            error = "file_transfer_failed";
            try {
                while(maxFileTransferRetries > 0) {
                p = Runtime.getRuntime().exec(cmd);
                errorGobbler = new StreamGobbler(p.getErrorStream(), "ERROR");
                outputGobbler = new StreamGobbler(p.getInputStream(), "OUTPUT");
                errorGobbler.start();
                outputGobbler.start();
                
                    
                exitValue = p.waitFor();
                    
                exitValueString = String.valueOf(exitValue);
                p.destroy();
                
                File f = new File(jobRequest.receivingFile);
                
                if(f.exists()) {
                    error="";
                    break;
                } else {
                    System.out.println("File transfer failed, receiving file does not exist");
                    maxFileTransferRetries--;
                }
                
                
                }
                
            } catch (IOException ex) {
                Logger.getLogger(Slave.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                        Logger.getLogger(Slave.class.getName()).log(Level.SEVERE, null, ex);
                    }
                
            
        } else {
            
        
        try {
            while (maxStrangeRetries > 0) {
                p = Runtime.getRuntime().exec(cmd);
                errorGobbler = new StreamGobbler(p.getErrorStream(), "ERROR");
                outputGobbler = new StreamGobbler(p.getInputStream(), "OUTPUT");
                errorGobbler.start();
                outputGobbler.start();
                
                long ltimeout;
                if(jobRequest.timeout == 0) {
                    ltimeout = jobRequest.maxTimeout;
                } else {
                    ltimeout = jobRequest.timeout;
                }
                ltimeout = ltimeout * 1000;
                long startTime = System.currentTimeMillis();
                //System.out.println("timeout v is : "+ltimeout);
                while (p.isAlive() && jobRequest.monitor) {
                    
                    long elapsed = System.currentTimeMillis() - startTime;
                    System.out.println("elapsed: " + elapsed);
                    if((elapsed > ltimeout) && jobRequest.NodeExecutionThreadFinalized) {
                        System.out.println("time out killing...");
                        wr1.write("timeout reached at " + ltimeout);
                        wr1.newLine();
                        wr1.flush();
                        p.destroy();
                        sleep(1000);
                        if (p.isAlive()) {
                            p.destroyForcibly();
                        }
                        jobResponse.timeOutKilled = true;
                        return "timeout";                        
                    }
                    long availMemory = getAvailMemory();
                    //System.out.println("avail memory: "+availMemory);
                    if((availMemory < jobRequest.killProcessAvailMemoryLimit) && jobRequest.NodeExecutionThreadFinalized) {
                        System.out.println("oom killing...");
                        wr1.write("less than avail mem limit, terminating");
                        wr1.newLine();
                        wr1.flush();
                        p.destroy();
                        sleep(1000);
                        if (p.isAlive()) {
                            p.destroyForcibly();
                        }
                        forceKillScreen();
                        jobResponse.outOfMemoryKilled = true;
                        return "less than avail mem limit, terminating";
                    }
                    
                    if(p.isAlive()) {
                        sleep(2000);
                    }
                }

                exitValue = p.waitFor();
                exitValueString = String.valueOf(exitValue);
                p.destroy();
                try {
                    wr1.write("exit value: " + exitValueString + " report: " + outputGobbler.output.toString() + " error: " + errorGobbler.output.toString());
                    wr1.newLine();
                    wr1.flush();
                } catch (IOException ex) {
                    Logger.getLogger(Slave.class.getName()).log(Level.SEVERE, null, ex);
                }
                error = errorGobbler.output.toString();

                if (exitValue == 0) {
                    break;
                }
                
                else if (exitValue != 0 && (errorGobbler.output.toString().equals("") || errorGobbler.output.toString().equals(" "))) {
                    maxStrangeRetries = maxStrangeRetries - 1;
                    wr1.write("process crashed with empty error log.. retring attempt: " + maxStrangeRetries);
                    wr1.newLine();
                    wr1.flush();
                    mscError = "process crashed with empty error log.. retrying attempt " + String.valueOf(maxStrangeRetries);
                    sleep(10000);
                    error = mscError;
                } else {
                    break;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("exception " + e);
        }}
        return error;
    }

    public Slave(int id, JobRequest jobRequest, BufferedWriter wr1, String wrapper, String monitor, String baseDir, String componentRuntimeLogs, String diskLog, String topLog) {
        this.wr1 = wr1;
        this.jobRequest = jobRequest;
        this.jobResponse = new JobResponse();
        this.jobResponse.jobRequest = this.jobRequest;
        this.wrapper = wrapper;
        this.baseDir = baseDir;
        this.topLog = topLog;
        this.diskLog = diskLog;
        this.componentRuntimeLogs = componentRuntimeLogs;
        this.id = id;
    }

    //   public void prepareRuntimeLog(JSONObject obj, JSONObject incoming, ArrayList<String> outputSizes, long runTime, JSONObject resourceLogs) throws IOException {
//        JSONArray runTimeLog = new JSONArray();
//        JSONObject entry = new JSONObject();
//    entry.put (
//
//    "outputsRealFileSizesInB", outputSizes.get(0));
//    entry.put (
//
//    "outputsRealFileSizesCustom", outputSizes.get(1));
//        entry.put("inputsRealFileSizesInB", incomingJSON.get("inputsRealFileSizesInB"));
//        entry.put("inputsRealFileSizesCustom", incomingJSON.get("inputsRealFileSizesCustom"));
//        entry.put("resourceName", incoming.get("resourceName"));

//        entry.put("slotsUsed", incoming.get("slotsUsed"));
//        entry.put("componentName", incoming.get("componentName"));
//    entry.put (
//
//    "runtimeInMs", String.valueOf(runTime));

//        entry.put("totalSystemCPUs", incomingJSON.get("totalSystemCPUs"));
//        entry.put("systemRAMSizeInMB", incomingJSON.get("systemRAMSizeInMB"));
//        entry.put("cpuSingleThreadedBenchmark", incomingJSON.get("cpuSingleThreadedBenchmark"));
//        entry.put("cpuMultiThreadedBenchmark", incomingJSON.get("cpuMultiThreadedBenchmark"));
//        if (incomingJSON.containsKey("threadsAssigned")) {
//            entry.put("threadsAssigned", incomingJSON.get("threadsAssigned"));
//        }
//        if (runTime > 2000) {
//            entry.put("ComponentResouceUseLogs", resourceLogs);
//        }
//
//        entry.put("date", (new Date()).toString().replace(" ", "").replace(":", "_"));
        
//runTimeLog.add(entry);
        
        //obj.put("runTimeLog", runTimeLog);
        //System.out.println(obj.toJSONString());
 //   }

    public void prepareResponse(Boolean success, String error, long runTime) throws IOException {
        //JSONObject obj = new JSONObject();
        //ArrayList<String> outputSizesInB = new ArrayList<String>();
        //JSONObject resourceLogs = new JSONObject();
        
        
        
        
        

//        if (!jobRequest.outputDataFileIds.isEmpty()) {
//            outputSizesInB = assignOutputFileSizes();
//        } else {
//            outputSizesInB.add("0");
//            outputSizesInB.add("0");
//        }
//        if (incomingJSON.containsKey(("InputDataFiles"))) {
//            obj.put("InputDataFiles", incomingJSON.get("InputDataFiles"));
//        }

//        if (incomingJSON.containsKey("monitor") && (boolean) incomingJSON.get("monitor")) {
//            obj.put("monitor", incomingJSON.get("monitor"));
//        }
        if (success) {
            jobResponse.success = true;
            assignOutputFileSizes();
            //obj.put("success", true);
        } else {
            jobResponse.success = false;
            jobResponse.error = error;
//            obj.put("success", false);
//            obj.put("error", error);
        }

        //success &&  removed run only if success
        if (jobRequest.monitor) {
            if (runTime > 2000) {
            buildProcessLogsToJson(runTime);
        }
            
            //prepareRuntimeLog(obj, incomingJSON, outputSizesInB, runTime, resourceLogs);

//            jobResponse.outputDataFileSizesBytes = outputSizesInB.get(0);
//            jobResponse.outputDataFileSizesCustom = outputSizesInB.get(1);
            jobResponse.runtime = runTime;
//            if (runTime > 2000) {
//                
//                entry.put("ComponentResouceUseLogs", resourceLogs);
//            }

        }

        jobResponse.dateCompleted = (new Date());
//        obj.put("commandReceived", incomingJSON.get("command"));
//        obj.put("NodeExecutionThreadId", incomingJSON.get("NodeExecutionThreadId"));
//        obj.put("NodeID", incomingJSON.get("NodeID"));

//        if (incomingJSON.containsKey("NodeExecutionThreadFinalized")) {
//            obj.put("NodeExecutionThreadFinalized", true);
//        }
        //return obj;
    }

    public void assignOutputFileSizes() throws IOException {
        //JSONArray msg = (JSONArray) incomingJSON.get("OutputDataFiles");
//        ArrayList<String> ret = new ArrayList<String>();
//        StringBuilder bytesSB = new StringBuilder();
//        StringBuilder customSB = new StringBuilder();

        //ArrayList<JSONObject> outputDataFiles = msg;
        jobResponse.jobOutputFileMetrics = new HashMap<String, Map<String,String>>();
        for (int i = 0; i < jobRequest.outputDataFileIds.size(); i++) {
           
            Map<String, String> metrics = jobResponse.jobOutputFileMetrics.get(String.valueOf(jobRequest.outputDataFileIds.get(i)));
            metrics = new HashMap<String, String>();
            
            DataFileEvaluationFunctions.selector(jobRequest.outputDataFilePaths.get(i), metrics, jobRequest.inputOutputFileAssessment, jobRequest);
            jobResponse.jobOutputFileMetrics.put(String.valueOf(jobRequest.outputDataFileIds.get(i)), metrics);
                    
             
            //JSONObject dataFile = outputDataFiles.get(i);
//            File currentOutput = new File(jobRequest.outputDataFilePaths.get(i));
//            String fileSize;
//
//            if (!currentOutput.exists()) {
//                Path folder = currentOutput.getParentFile().toPath();
//                fileSize = String.valueOf(Files.walk(folder)
//                        .filter(p -> p.toFile().isFile())
//                        .mapToLong(p -> p.toFile().length())
//                        .sum());
//                bytesSB.append(fileSize).append(" ");
//            } else {
//                fileSize = String.valueOf(currentOutput.length());
//                bytesSB.append(fileSize).append(" ");
//            }
//
//            if (jobRequest.inputOutputFileAssessment) {
                //customSB.append(DataFileEvaluationFunctions.selector(jobRequest.outputDataFilePaths.get(i), jobRequest, jobResponse)).append(" ");
           // }
            //dataFile.put("sizeInBytes", fileSize);

        }

    }

    public void run() {
        int maxTries = 1;
        if(jobRequest.jobIsFileTransfer) {
            maxTries =10;
        }
        int retries = maxTries;
        boolean wasRetried = false;
        //boolean timeOutReached = false;
        try {
            long initTime = System.currentTimeMillis();
            long runTime = 0;
            startedAtSecond = (System.currentTimeMillis() / 1000L) - Client.topInitTimeStamp;
            startedAtUnixTimestamp = System.currentTimeMillis() / 1000L;
            String proc = "";
            while (retries > 0 && !jobResponse.timeOutKilled && !jobResponse.outOfMemoryKilled) {

                runTime = 0;
                String command = jobRequest.command;               
                System.out.println("Incoming JSON: " + gson.toJson(jobRequest));
                wr1.newLine();
                wr1.write("-----------------------");
                wr1.newLine();
                wr1.write(command);
                wr1.newLine();
                wr1.write("Started @" + (new Date()).toString());
                wr1.newLine();
                wr1.write(".......");
                wr1.newLine();

                wr1.write(baseDir);
                wr1.newLine();
                wr1.flush();

                if (jobRequest.monitor) {

                    String componentID = jobRequest.componentID;
                    wr1.newLine();
                    wr1.write("Monitor is activated");
                    wr1.newLine();

                    String chCommand = command.replace("sh;-c;", "");
                    String errorLog = baseDir + "/" + componentID + ".errorLogTmp";
                    processLog = componentRuntimeLogs + "/logProcesses_" + componentID + ".process";
                    screenName = "sreenName_" + componentID;
                    chCommand = "sh;-c;bash " + wrapper + " " + "\"" + chCommand + " 2>" + errorLog + " " + "\"" + " " + "\"" + screenName + "\"" + " " + "\"" + processLog + "\"";

                    wr1.write("AlteredCommand: " + chCommand);
                    wr1.newLine();
                    wr1.flush();

                    command = chCommand;

                    initTime = System.currentTimeMillis();
                    proc = executeBashScript(command.split(";"));
                    runTime = System.currentTimeMillis() - initTime;

                    File errorLogFile = new File(errorLog);
                    StringBuilder sb = new StringBuilder();
                    if (errorLogFile.exists()) {// && !(((String) incomingJSON.get("componentName")).equals("mclBlastProtein"))) {
                        BufferedReader errorReader = new BufferedReader(new FileReader(errorLogFile));
                        String line;
                        while ((line = errorReader.readLine()) != null) {
                            sb.append(line);
                        }
                        if (!sb.toString().contains("http://www.library.uu.nl/digiarchief/dip/diss/1895620/full.pdf")) {
                            String err = sb.toString();
                            String killed = "";
                            if (err.contains("Killed")) {
                                killed = "Killed";
                            }

                            err = err.replace("\"", "");
                            if (err.length() > 1000) {
                                err = "Killed " + err.substring(0, 1000);
                            }
                            proc = proc + killed + err;
                            errorReader.close();
                            File tmpErrorLog = new File(errorLog);
                            tmpErrorLog.delete();
                            wr1.write("ERROR LOG: " + proc);
                            wr1.newLine();
                            wr1.flush();
                        }
                    }

                    try {
                        sleep(700);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(Slave.class.getName()).log(Level.SEVERE, null, ex);
                    }

                } else {
                    proc = executeBashScript(command.split(";"));
                }
                if (!proc.isEmpty()) {
                    wr1.write("!!!!!!!!!!!!!!!!!!!!!FAILURE @ " + gson.toJson(jobRequest) + "  " + proc);
                    wr1.newLine();
                    wr1.write("will retry before reporting.. retrying attempt: " + retries);
                    retries = retries - 1;
                    wr1.newLine();
                    wr1.flush();
                    wasRetried = true;
//                    if (proc.equals("timeout")) {
//                        timeOutReached = true;
//                    }
                    try {
                        sleep(5000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(Slave.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    break;
                }

            }

            wr1.write("Finished @" + (new Date()).toString());
            wr1.newLine();
            long t4 = System.currentTimeMillis();
            wr1.write("Duration @" + String.valueOf(t4 - initTime));
            wr1.newLine();
            wr1.write("-----------------------");
            wr1.newLine();
            wr1.flush();

            if (!proc.isEmpty()) {
                wr1.write("!!!!!!!!!!!!!!!!!!!!!FAILURE @ " + gson.toJson(jobRequest) + "  " + proc);
                wr1.newLine();
                wr1.flush();

                //JSONObject jsonreply = prepareResponse(false, proc, runTime);
                prepareResponse(false, proc, runTime);
                if (wasRetried) {
                    jobResponse.wasRetriedTimes = maxTries - retries;
                }
                if (!mscError.equals("")) {
                    jobResponse.mscError = mscError;
                }
                
                if(jobResponse.timeOutKilled) {
                    forceKillScreen();
                }

                String reply = gson.toJson(jobResponse);
                wr1.write("SENDING: " + reply);
                wr1.newLine();
                wr1.flush();

                Client.talker(reply, masterAddress, masterJobPort);

            } else {
                wr1.write("SUCCESS @ " + gson.toJson(jobRequest));
                wr1.newLine();
                wr1.flush();

                prepareResponse(true, proc, runTime);
                if (wasRetried) {
                    jobResponse.wasRetriedTimes = maxTries - retries;
                }
                if (!mscError.equals("")) {
                    jobResponse.mscError = mscError;
                }

                String reply = gson.toJson(jobResponse);
                wr1.write("SENDING: " + reply);
                wr1.newLine();
                wr1.flush();

                System.out.println("sending :" + "SUCCESS @ " + reply);

                Client.talker(reply, masterAddress, masterJobPort);
                System.gc();
            }

        } catch (IOException ex) {
            Logger.getLogger(Slave.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void talker(String message, String serverName, String portString) {

        int port = Integer.parseInt(portString);
        try {
            Socket client = new Socket(serverName, port);
            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out = new DataOutputStream(outToServer);
            out.writeUTF(message);
            out.close();
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    
    public static long getAvailMemory() throws IOException {
        String[] cmd = {
            "/bin/sh",
            "-c",
            "free | grep Mem | awk '{print $7}'"
        };
        Process proc = Runtime.getRuntime().exec(cmd);
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        long availMemory = Long.parseLong(stdInput.readLine());
        return availMemory;
    }
    
    public void forceKillScreen() throws IOException {
        String[] cmd = {
            "/bin/sh",
            "-c",
            "screen -S " + screenName +" -X kill"
        };
        Process proc = Runtime.getRuntime().exec(cmd);
    }

}
