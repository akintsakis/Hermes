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
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

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
    JSONObject incomingJSON;
    BufferedWriter wr1;

    public long startedAtSecond;
    public long startedAtUnixTimestamp;

    String processLog = "";
    String mscError = "";
    String errorLog = "";
    long timeOut;

    public JSONObject buildProcessLogsToJson(double runTime) {
        JSONObject entry = new JSONObject();
        try {
            ParseTop.logstoJson(entry, processLog, topLog, diskLog, startedAtSecond, startedAtUnixTimestamp, runTime);
        } catch (IOException ex) {
            Logger.getLogger(Slave.class.getName()).log(Level.SEVERE, null, ex);
        }

        return entry;
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
        int exitValue = 1;
        try {
            while (maxStrangeRetries > 0) {
                p = Runtime.getRuntime().exec(cmd);
                errorGobbler = new StreamGobbler(p.getErrorStream(), "ERROR");
                outputGobbler = new StreamGobbler(p.getInputStream(), "OUTPUT");
                errorGobbler.start();
                outputGobbler.start();

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
                } else if (exitValue == 1 && (errorGobbler.output.toString().equals("") || errorGobbler.output.toString().equals(" "))) {
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
        }
        return error;
    }

    public Slave(int id, JSONObject incomingJSON, BufferedWriter wr1, String wrapper, String monitor, String baseDir, String componentRuntimeLogs, String diskLog, String topLog) {
        this.wr1 = wr1;
        this.incomingJSON = incomingJSON;
        this.wrapper = wrapper;
        this.baseDir = baseDir;
        this.topLog = topLog;
        this.diskLog = diskLog;
        this.componentRuntimeLogs = componentRuntimeLogs;
        this.id = id;
    }

    public void prepareRuntimeLog(JSONObject obj, JSONObject incoming, ArrayList<String> outputSizes, long runTime, JSONObject resourceLogs) throws IOException {
        JSONArray runTimeLog = new JSONArray();
        JSONObject entry = new JSONObject();

        entry.put("outputsRealFileSizesInB", outputSizes.get(0));
        entry.put("outputsRealFileSizesCustom", outputSizes.get(1));
        entry.put("inputsRealFileSizesInB", incomingJSON.get("inputsRealFileSizesInB"));
        entry.put("inputsRealFileSizesCustom", incomingJSON.get("inputsRealFileSizesCustom"));
        entry.put("resourceName", incoming.get("resourceName"));

        entry.put("slotsUsed", incoming.get("slotsUsed"));
        entry.put("componentName", incoming.get("componentName"));
        entry.put("runtimeInMs", String.valueOf(runTime));

        entry.put("totalSystemCPUs", incomingJSON.get("totalSystemCPUs"));
        entry.put("systemRAMSizeInMB", incomingJSON.get("systemRAMSizeInMB"));
        entry.put("cpuSingleThreadedBenchmark", incomingJSON.get("cpuSingleThreadedBenchmark"));
        entry.put("cpuMultiThreadedBenchmark", incomingJSON.get("cpuMultiThreadedBenchmark"));
        if (incomingJSON.containsKey("threadsAssigned")) {
            entry.put("threadsAssigned", incomingJSON.get("threadsAssigned"));
        }
        if (runTime > 2000) {
            entry.put("ComponentResouceUseLogs", resourceLogs);
        }

        entry.put("date", (new Date()).toString().replace(" ", "").replace(":", "_"));
        runTimeLog.add(entry);
        obj.put("runTimeLog", runTimeLog);
        System.out.println(obj.toJSONString());
    }

    public JSONObject prepareResponse(Boolean success, String error, long runTime) throws IOException {
        JSONObject obj = new JSONObject();
        ArrayList<String> outputSizesInB = new ArrayList<String>();
        JSONObject resourceLogs = new JSONObject();
        if (runTime > 2000) {
            resourceLogs = buildProcessLogsToJson(runTime);
        }

        if (incomingJSON.containsKey(("OutputDataFiles"))) {
            outputSizesInB = assignOutputFileSizes(incomingJSON);
        }
        if (incomingJSON.containsKey(("InputDataFiles"))) {
            obj.put("InputDataFiles", incomingJSON.get("InputDataFiles"));
        }

        if (incomingJSON.containsKey("monitor") && (boolean) incomingJSON.get("monitor")) {
            obj.put("monitor", incomingJSON.get("monitor"));
        }

        if (success) {
            obj.put("success", true);
        } else {
            obj.put("success", false);
            obj.put("error", error);
        }

        //success &&  removed run only if success
        if (incomingJSON.containsKey("monitor") && (boolean) incomingJSON.get("monitor")) {
            prepareRuntimeLog(obj, incomingJSON, outputSizesInB, runTime, resourceLogs);
        }

        obj.put("commandReceived", incomingJSON.get("command"));
        obj.put("timestamp", (new Date()).toString());
        obj.put("NodeExecutionThreadId", incomingJSON.get("NodeExecutionThreadId"));
        obj.put("NodeID", incomingJSON.get("NodeID"));

        if (incomingJSON.containsKey("NodeExecutionThreadFinalized")) {
            obj.put("NodeExecutionThreadFinalized", true);
        }

        return obj;
    }

    public ArrayList<String> assignOutputFileSizes(JSONObject incomingJSON) throws IOException {
        JSONArray msg = (JSONArray) incomingJSON.get("OutputDataFiles");
        ArrayList<String> ret = new ArrayList<String>();
        StringBuilder bytesSB = new StringBuilder();
        StringBuilder customSB = new StringBuilder();

        ArrayList<JSONObject> outputDataFiles = msg;
        for (int i = 0; i < outputDataFiles.size(); i++) {
            JSONObject dataFile = outputDataFiles.get(i);
            File currentOutput = new File((String) dataFile.get("path"));
            String fileSize;

            if (!currentOutput.exists()) {
                Path folder = currentOutput.getParentFile().toPath();
                fileSize = String.valueOf(Files.walk(folder)
                        .filter(p -> p.toFile().isFile())
                        .mapToLong(p -> p.toFile().length())
                        .sum());
                bytesSB.append(fileSize).append(" ");
            } else {
                fileSize = String.valueOf(currentOutput.length());
                bytesSB.append(fileSize).append(" ");
            }

            if (incomingJSON.containsKey("inputOutputFileAssessment") && (boolean) incomingJSON.get("inputOutputFileAssessment")) {
                customSB.append(DataFileEvaluationFunctions.selector((String) dataFile.get("path"), incomingJSON)).append(" ");
            }
            dataFile.put("sizeInBytes", fileSize);

        }
        ret.add(bytesSB.toString());
        ret.add(customSB.toString());
        return ret;
    }

    public void run() {
        int maxRetries = 5;
        int retries = maxRetries;
        boolean wasRetried = false;
        try {
            long initTime = System.currentTimeMillis();
            long runTime = 0;
            startedAtSecond = (System.currentTimeMillis() / 1000L) - Client.topInitTimeStamp;
            startedAtUnixTimestamp = System.currentTimeMillis() / 1000L;
            String proc = "";
            while (retries > 0) {

                runTime = 0;
                String command = (String) incomingJSON.get("command");
                timeOut = Long.valueOf((String) incomingJSON.get("timeout"));
                System.out.println("Incoming JSON: " + incomingJSON.toJSONString());
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

                if (incomingJSON.containsKey("monitor") && (boolean) incomingJSON.get("monitor")) {

                    String componentID = (String) incomingJSON.get("componentID");
                    wr1.newLine();
                    wr1.write("Monitor is activated");
                    wr1.newLine();

                    String chCommand = command.replace("sh;-c;", "");
                    String errorLog = baseDir + "/" + componentID + ".errorLogTmp";
                    processLog = componentRuntimeLogs + "/logProcesses_" + componentID + ".process";
                    chCommand = "sh;-c;bash " + wrapper + " " + "\"" + chCommand + " 2>" + errorLog + " " + "\"" + " " + "\"" + "sreenName_" + componentID + "\"" + " " + "\"" + processLog + "\"";

                    wr1.write("AlteredCommand: " + chCommand);
                    wr1.newLine();
                    wr1.flush();

                    command = chCommand;

                    initTime = System.currentTimeMillis();
                    proc = executeBashScript(command.split(";"));
                    runTime = System.currentTimeMillis() - initTime;

                    File errorLogFile = new File(errorLog);
                    StringBuilder sb = new StringBuilder();
                    if (errorLogFile.exists() && !(((String) incomingJSON.get("componentName")).equals("mclBlastProtein"))) {
                        BufferedReader errorReader = new BufferedReader(new FileReader(errorLogFile));
                        String line;
                        while ((line = errorReader.readLine()) != null) {
                            sb.append(line);
                        }
                        proc = proc + sb.toString();
                        errorReader.close();
                        File tmpErrorLog = new File(errorLog);
                        tmpErrorLog.delete();
                        wr1.write("ERROR LOG: " + proc);
                        wr1.newLine();
                        wr1.flush();
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
                    wr1.write("!!!!!!!!!!!!!!!!!!!!!FAILURE @ " + incomingJSON + "  " + proc);
                    wr1.newLine();
                    wr1.write("will retry before reporting.. retrying attempt: " + retries);
                    retries = retries - 1;
                    wr1.newLine();
                    wr1.flush();
                    wasRetried = true;
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
                wr1.write("!!!!!!!!!!!!!!!!!!!!!FAILURE @ " + incomingJSON + "  " + proc);
                wr1.newLine();
                wr1.flush();
                JSONObject jsonreply = prepareResponse(false, proc, runTime);
                if (wasRetried) {
                    jsonreply.put("wasRetriedTimes", String.valueOf(maxRetries - retries));
                }
                if (!mscError.equals("")) {
                    jsonreply.put("mscError", mscError);
                }

                String reply = jsonreply.toJSONString();
                wr1.write("SENDING: " + reply);
                wr1.newLine();
                wr1.flush();

                Client.talker(reply, masterAddress, masterJobPort);

            } else {
                wr1.write("SUCCESS @ " + incomingJSON);
                wr1.newLine();
                wr1.flush();

                JSONObject jsonreply = prepareResponse(true, proc, runTime);
                if (wasRetried) {
                    jsonreply.put("wasRetriedTimes", String.valueOf(maxRetries - retries));
                }
                if (!mscError.equals("")) {
                    jsonreply.put("mscError", mscError);
                }

                String reply = jsonreply.toJSONString();
                wr1.write("SENDING: " + reply);
                wr1.newLine();
                wr1.flush();

                System.out.println("sending :" + "SUCCESS @ " + reply);

                Client.talker(reply, masterAddress, masterJobPort);
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

}
