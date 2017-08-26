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
package Hermes;

import com.google.gson.Gson;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class IncomingCommunicatorThread extends Thread {

    Gson gson = new Gson();
    Lock NodeExecutionThreadQueue = new Lock();
    public ServerSocket socket;
    //WorkflowTree workflow;
    public ConcurrentHashMap<String, NodeExecutionThread> executingNow = new ConcurrentHashMap<String, NodeExecutionThread>();

    public void processIt(String response) throws InterruptedException {
        //JSONParser parser = new JSONParser();;

        JobResponse jobResponse = gson.fromJson(response, JobResponse.class);
        //JSONObject jsonResponse = (JSONObject) parser.parse(response);
        //String success = "FAILURE";

        if (jobResponse.jobRequest.NodeExecutionThreadId != null && !jobResponse.jobRequest.NodeExecutionThreadId.equals("null")) {
            NodeExecutionThread currentNodeExecutionThread = executingNow.get(jobResponse.jobRequest.NodeExecutionThreadId);
            String potentialError = "None";
            if (jobResponse.error != null) {
                potentialError = jobResponse.error;
            } else if (jobResponse.mscError != null) {
                potentialError = "mscError detected, but successfully recovered";
            }

            HermesLogKeeper.logReceiver("+++RECEIVING+++ ::: " + currentNodeExecutionThread.node.component.executedOnResource.name + "  | component " + currentNodeExecutionThread.node.component.name + "| status " + jobResponse.success + " |error: " + potentialError);
            HermesLogKeeper.logReceiver("FULL reply: " + response);

            if (jobResponse.jobOutputFileMetrics != null && !jobResponse.jobOutputFileMetrics.isEmpty()) {
                for (Map.Entry<String, Map<String, String>> entry : jobResponse.jobOutputFileMetrics.entrySet()) {
                    DataFile.allDataFiles.get(entry.getKey()).metrics.putAll(entry.getValue());
                }
            }

            if (jobResponse.jobRequest.monitor) {
                //ArrayList<JSONObject> jsonLog = (JSONArray) jsonResponse.get("runTimeLog");
                //JSONObject entry = jsonLog.get(0);
                File runtimeLogFile = new File(Configuration.globalConfig.locationOfHermesRootFolder + "/ComponentExecutionLogs/" + jobResponse.jobRequest.componentName + "/");
                runtimeLogFile.mkdirs();
                int numberOfexistingFiles = runtimeLogFile.listFiles().length;
                try {
                    BufferedWriter runtimeLogWriter = new BufferedWriter(new FileWriter(runtimeLogFile.getAbsolutePath() + "/" + String.valueOf(numberOfexistingFiles) + "_" + (new Date()).toString().replace(" ", "_").replace(":", "_")));
                    runtimeLogWriter.write(gson.toJson(jobResponse));
                    runtimeLogWriter.close();
                } catch (IOException ex) {
                    Logger.getLogger(NodeExecutionThread.class.getName()).log(Level.SEVERE, null, ex);
                }

                if (jobResponse.jobRequest.NodeID != null) {
                    //ArrayList<JSONObject> jsonLog = (JSONArray) jsonResponse.get("runTimeLog");
                    //JSONObject entry = jsonLog.get(0);
                    TreeNode node = Hermes.hermes.workflow.allNodes.get(Integer.valueOf(jobResponse.jobRequest.NodeID));

//                    node.component.setOutputsRealFileSizesInB(jobResponse.outputDataFileSizesBytes);
//                    if (jobResponse.outputDataFileSizesCustom != null && Configuration.globalConfig.inputOutputFileAssessment) {
//                        node.component.setOutputsRealFileSizesCustom(jobResponse.outputDataFileSizesCustom);
//                    }
                }
            }

            currentNodeExecutionThread.NodeExecutionThreadQueue.wake();
            if (jobResponse.jobRequest.NodeExecutionThreadFinalized) {
                Hermes.hermes.workflow.threadslock.lock();
                //remove extra runs
                if (!jobResponse.success || currentNodeExecutionThread.node.component.extraRuns > 0) {
                    //System.out.println("Putting back to waiting queue...");
                    currentNodeExecutionThread.node.component.extraRuns--;
                    currentNodeExecutionThread.node.component.executionCompleted = false;
                    currentNodeExecutionThread.node.component.lastExecutionFailed = true;

                } else {
                    currentNodeExecutionThread.node.component.lastExecutionFailed = false;
                    currentNodeExecutionThread.node.component.executionCompleted = true;
                    buildFileRetrieveCommand(currentNodeExecutionThread);
                }
                Hermes.hermes.workflow.executionComplete.add(currentNodeExecutionThread);
                Hermes.hermes.workflow.threadslock.unlock();
                Hermes.hermes.workflow.masterlock.wake();
                executingNow.remove(jobResponse.jobRequest.NodeExecutionThreadId);
                System.out.println();
                System.out.println((new Date()).toString() + " - Component: " + currentNodeExecutionThread.node.component.id + "_" + currentNodeExecutionThread.node.component.name + " finished on site: " + currentNodeExecutionThread.node.component.executedOnResource.name + " statusSuccess: " + jobResponse.success + " error: " + potentialError);
            }
        }

    }

    public String buildFileRetrieveCommand(NodeExecutionThread thread) {
        talker(String.valueOf(thread.node.component.id), "localhost", Configuration.globalConfig.fileRetrieverListeningPortOnMaster);
        return "";
    }

    IncomingCommunicatorThread() throws IOException {
        int maxRetries = Configuration.globalConfig.maxPortRetries;
        int listeningPort = Configuration.globalConfig.commsThreadListeningPortOnMaster;
        while (maxRetries > 0) {
            try {
                socket = new ServerSocket(listeningPort);
                break;
            } catch (BindException ex) {
                maxRetries--;
                System.out.println("Port " + listeningPort + " in use.... retrying +1");
                listeningPort++;
            }
        }
    }

    public static void talker(String message, String serverName, Integer port) {
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

    public void run() {

        while (true) {
            try {
                Socket server = socket.accept();
                DataInputStream in = new DataInputStream(server.getInputStream());
                String response = in.readUTF();
                server.close();
                if (response.contains(Configuration.globalConfig.intraMasterTerminationSignal)) {
                    System.out.println("Comms Thread received termination signal");
                    break;
                } else {
                    processIt(response);
                }
            } catch (IOException ex) {
                Logger.getLogger(IncomingCommunicatorThread.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(IncomingCommunicatorThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
