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
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class IncomingCommunicatorThread extends Thread {

    Lock NodeExecutionThreadQueue = new Lock();
    public ServerSocket socket;
    //WorkflowTree workflow;
    public ConcurrentHashMap<String, NodeExecutionThread> executingNow = new ConcurrentHashMap<String, NodeExecutionThread>();

    public void processIt(String response) throws InterruptedException {
        JSONParser parser = new JSONParser();;
        try {
            JSONObject jsonResponse = (JSONObject) parser.parse(response);
            String success = "FAILURE";
            if (jsonResponse.containsKey("success") && (boolean) jsonResponse.get("success")) {
                success = "SUCCESS";
            }

            if (jsonResponse.containsKey("NodeExecutionThreadId") && !((String) jsonResponse.get("NodeExecutionThreadId")).equals("null")) {
                NodeExecutionThread currentNodeExecutionThread = executingNow.get((String) jsonResponse.get("NodeExecutionThreadId"));
                String potentialError = "None";
                if (jsonResponse.containsKey("error")) {
                    potentialError = jsonResponse.get("error").toString();
                } else if (jsonResponse.containsKey("mscError")) {
                    potentialError = "mscError detected, but successfully recovered";
                }
                
                HermesLogKeeper.logReceiver("+++RECEIVING+++ ::: " + currentNodeExecutionThread.node.component.executedOnResource.name + "  | component " + currentNodeExecutionThread.node.component.name + "| status " + success + " |error: " + potentialError);
                HermesLogKeeper.logReceiver("FULL reply: " + jsonResponse.toJSONString());
         
                if (jsonResponse.containsKey("runTimeLog")) {                    
                    ArrayList<JSONObject> jsonLog = (JSONArray) jsonResponse.get("runTimeLog");
                    JSONObject entry = jsonLog.get(0);
                    File runtimeLogFile = new File(Configuration.globalConfig.locationOfHermesRootFolder + "/ComponentExecutionLogs/" + (String) entry.get("componentName") + "/");
                    runtimeLogFile.mkdirs();
                    int numberOfexistingFiles = runtimeLogFile.listFiles().length;
                    try {
                        BufferedWriter runtimeLogWriter = new BufferedWriter(new FileWriter(runtimeLogFile.getAbsolutePath() + "/" + String.valueOf(numberOfexistingFiles)+"_"+(new Date()).toString().replace(" ", "_").replace(":", "_")));
                        runtimeLogWriter.write(entry.toJSONString());
                        runtimeLogWriter.close();
                    } catch (IOException ex) {
                        Logger.getLogger(NodeExecutionThread.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    TreeNode node = Hermes.hermes.workflow.allNodes.get(Integer.valueOf((String) jsonResponse.get("NodeID")));
                    node.component.setOutputsRealFileSizesInB((String) entry.get("outputsRealFileSizesInB"));
                    if (entry.containsKey("outputsRealFileSizesCustom") && Configuration.globalConfig.inputOutputFileAssessment) {
                        node.component.setOutputsRealFileSizesCustom((String) entry.get("outputsRealFileSizesCustom"));
                    }
                }
                
                currentNodeExecutionThread.NodeExecutionThreadQueue.wake();
                if (jsonResponse.containsKey("NodeExecutionThreadFinalized")) {
                    currentNodeExecutionThread.node.component.executionCompleted = true;
                    buildFileRetrieveCommand(currentNodeExecutionThread);
                    Hermes.hermes.workflow.threadslock.lock();
                    Hermes.hermes.workflow.executionComplete.add(currentNodeExecutionThread);
                    Hermes.hermes.workflow.threadslock.unlock();
                    Hermes.hermes.workflow.masterlock.wake();
                    executingNow.remove((String) jsonResponse.get("NodeExecutionThreadId"));
                    System.out.println();
                    System.out.println((new Date()).toString() + " - Component: " + currentNodeExecutionThread.node.component.id + "_" + currentNodeExecutionThread.node.component.name + " finished on site: " + currentNodeExecutionThread.node.component.executedOnResource.name + " status: " + success + " error: " + potentialError);
                }
            }

        } catch (ParseException ex) {
            System.out.println("Incoming Communicatior received invalid message... ignoring");
            //Logger.getLogger(NodeExecutionThread.class.getName()).log(Level.SEVERE, null, ex);
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
