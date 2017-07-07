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

/* This class represents the FileRetriever thread, tasked with retrieving the job
   output files from the execution sites.
*/
package Hermes;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import org.json.simple.JSONObject;

public class FileRetriever extends Thread {

    public ServerSocket socket;
    static String workflowResultsFolder = System.getProperty("user.home") + "/hermes_workflow_results_" + Configuration.globalConfig.currentDate + "/";
    Long totalOutputFilesRetrieved = 0L;
    //Connection conn;

    FileRetriever() throws IOException {
        int maxRetries = Configuration.globalConfig.maxPortRetries;
        int listeningPort = Configuration.globalConfig.fileRetrieverListeningPortOnMaster;
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

        File folder = new File(workflowResultsFolder);
        folder.mkdirs();
    }

    public static String executeBashScript(String[] cmd) {
        StringBuilder output1 = new StringBuilder();
        Process p;
        try {
            p = Runtime.getRuntime().exec(cmd);
            p.waitFor();
            BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line = "";
            while ((line = error.readLine()) != null) {
                output1.append(line).append("\n");
            }
            p.destroy();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("exception " + e);
        }
        return output1.toString();
    }

    public void retrieveLogsFromAllSites() throws IOException {

        System.out.println("Retrieving logs from all sites...");
        for (ExecutionSite res : Hermes.hermes.resources) {
            String resFolderPath = workflowResultsFolder + "/000Logs/" + res.name;
            File resourceFolder = new File(resFolderPath);
            resourceFolder.mkdirs();

            String localFileName = resFolderPath + "/clientLogfile.log";
            String scpcommand = "sh;-c;scp -c arcfour -i " + Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers + " -q -o StrictHostKeyChecking=no -C -r -P " + res.tunnelPortSSHContainers + " " + Configuration.globalConfig.containerUsernameForSSH + "@127.0.0.1:/home/user/" + DataFile.currentFolder + "/clientLogfile.log " + localFileName;
            HermesLogKeeper.logFileRetriever(scpcommand);
            scpTransferWithRetries(scpcommand, localFileName);

            localFileName = resFolderPath + "/dstatlog";
            scpcommand = "sh;-c;scp -c arcfour -i " + Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers + " -q -o StrictHostKeyChecking=no -C -r -P " + res.tunnelPortSSHContainers + " " + Configuration.globalConfig.containerUsernameForSSH + "@127.0.0.1:/home/user/" + DataFile.currentFolder + "/dstatlog " + localFileName;
            HermesLogKeeper.logFileRetriever(scpcommand);
            scpTransferWithRetries(scpcommand, localFileName);

        }

    }

    public boolean scpTransferWithRetries(String scpcommand, String localFileName) throws IOException {
        int numRetries = Configuration.globalConfig.numberOfFileRetrievalRetries;
        while (numRetries > 0) {
            String errorLog = executeBashScript(scpcommand.split(";"));
            numRetries = numRetries - 1;
            if (errorLog.equals("")) {
                File f = new File(localFileName);
                if (f.exists()) {
                    return true;
                }
            } else {
                System.out.println("File retrieval error occured, retrying... attempt:" + numRetries);
                System.out.println(errorLog);
                HermesLogKeeper.logFileRetriever("File retrieval error occured, retrying... attempt:" + numRetries);
                HermesLogKeeper.logFileRetriever(errorLog);
                numRetries = numRetries - 1;
            }
        }
        return false;
    }

    public void retrieveComponentOutputs() {

    }

    public void deleteNoLongerNeededComponentInputsAndOutputs(Component component) {
        ArrayList<DataFile> markedForDeletion = new ArrayList<DataFile>();

        for (int i = 0; i < component.inputDataFiles.size(); i++) {
            DataFile currentDataFile = component.inputDataFiles.get(i);
            boolean delete = true;

            for (Component check : Component.allComponents) {
                if (check.executionCompleted == false && check.inputDataFiles.contains(currentDataFile)) {
                    delete = false;
                }
            }
            if (delete) {
                if (!markedForDeletion.contains(currentDataFile)) {
                    markedForDeletion.add(currentDataFile);
                }

            }
        }

        for (int i = 0; i < component.outputDataFiles.size(); i++) {
            DataFile currentDataFile = component.outputDataFiles.get(i);
            boolean delete = true;
            for (Component check : Component.allComponents) {
                if (check.executionCompleted == false && check.inputDataFiles.contains(currentDataFile)) {
                    delete = false;
                }
            }
            if ((Configuration.globalConfig.uploadAllIntermediateResults || currentDataFile.isFinalOutput) && (!currentDataFile.retrievedToMaster)) {
                delete = false;
            }
            if (delete) {
                if (!markedForDeletion.contains(currentDataFile)) {
                    markedForDeletion.add(currentDataFile);
                }
            }
        }
        deleteFilesOnSites(markedForDeletion);
    }

    public void deleteFilesOnSites(ArrayList<DataFile> markedForDeletion) {
        for (int i = 0; i < markedForDeletion.size(); i++) {

            for (Map.Entry<ExecutionSite, String> entry : markedForDeletion.get(i).pathInResource.entrySet()) {
                ExecutionSite site = entry.getKey();
                String path = entry.getValue();
                //send a delete command to clientdaemon on site with monitor on off
                //System.out.println("Deleting ID: " + markedForDeletion.get(i).id + "_" + markedForDeletion.get(i).fileName + " on site:" + site.name);
                HermesLogKeeper.logFileRetriever("Deleting ID: " + markedForDeletion.get(i).id + "_" + markedForDeletion.get(i).fileName + " on site:" + site.name);
                String currentCommand = "sh;-c;rm -rf " + path;
                if (markedForDeletion.get(i).isDir) {
                    currentCommand = "sh;-c;rm -rf " + path.substring(0, path.lastIndexOf("/")) + "/";;
                }
                JSONObject jsonCommand = new JSONObject();
                jsonCommand.put("command", currentCommand);
                jsonCommand.put("NodeExecutionThreadId", "null");
                try {
                    sleep(500);
                } catch (InterruptedException ex) {
                    Logger.getLogger(FileRetriever.class.getName()).log(Level.SEVERE, null, ex);
                }

                site.passCommandToClientWithinContainer(jsonCommand.toJSONString(), null, 3);
            }
        }
    }

    public void processRequest(String componentId) throws IOException {
        Component justFinished = null;
        int componentIdInt = Integer.valueOf(componentId);
        for (int i = 0; i < Component.allComponents.size(); i++) {
            if (Component.allComponents.get(i).id == componentIdInt) {
                justFinished = Component.allComponents.get(i);
                break;
            }
        }
        if (justFinished == null) {
            System.out.println("FileRetriever received invalid component ID... ignoring request.");
        } else {
            Component component = justFinished;
            String pathInSource = "";
            ExecutionSite resource = component.executedOnResource;
            for (int i = 0; i < component.outputDataFiles.size(); i++) {
                if (Configuration.globalConfig.uploadAllIntermediateResults || component.outputDataFiles.get(i).isFinalOutput) {
                    pathInSource = component.outputDataFiles.get(i).pathInResource.get(component.executedOnResource);
                    String send = resource.tunnelPortSSHContainers + ";" + Configuration.globalConfig.containerUsernameForSSH + ";" + pathInSource + ";" + component.id + component.name;
                    completeRetrievalTransfer(send, component.outputDataFiles.get(i));
                }
            }
            deleteNoLongerNeededComponentInputsAndOutputs(justFinished);
        }
    }

    public void completeRetrievalTransfer(String command, DataFile dataFile) throws IOException {
        //System.out.println("%%%RetrieveCommand%%% :::" + command);
        String[] tmp = command.split(";");
        String containerTunnelPort = tmp[0];
        String containerUsername = tmp[1];
        String containerFilePath = tmp[2];
        String componentIdentifier = tmp[3];
        Path p = Paths.get(containerFilePath);
        p.getFileName();
        //String localFileName = workflowResultsFolder + "/";//+ componentIdentifier + "/";
        String localFileName = workflowResultsFolder + componentIdentifier + "/";
        File f = new File(localFileName);
        f.mkdirs();
        String containerParentFolderPath = containerFilePath.substring(0, containerFilePath.lastIndexOf("/")) + "/";
        String retrievedFileName = containerFilePath.substring(containerFilePath.lastIndexOf("/") + 1, containerFilePath.length());
        //System.out.println("retrieved file name: "+retrievedFileName);
//int numRetries = Configuration.numberOfFileRetrievalRetries;
        String scpcommand = "sh;-c;scp -c arcfour -i " + Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers + " -q -o StrictHostKeyChecking=no -C -r -P " + containerTunnelPort + " " + containerUsername + "@127.0.0.1:" + containerFilePath + " " + localFileName;
        //System.out.println("Retrieving: " + dataFile.id + "_" + dataFile.fileName + " isDirectory:" + dataFile.isDir);
        HermesLogKeeper.logFileRetriever("Retrieving: " + dataFile.id + "_" + dataFile.fileName + " isDirectory:" + dataFile.isDir);

        if (dataFile.isDir) {
            scpcommand = "sh;-c;scp -c arcfour -i " + Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers + " -q -o StrictHostKeyChecking=no -C -r -P " + containerTunnelPort + " " + containerUsername + "@127.0.0.1:" + containerParentFolderPath + " " + localFileName;
        }
        HermesLogKeeper.logFileRetriever(scpcommand);
        boolean success = scpTransferWithRetries(scpcommand, localFileName);
        if (success) {
            dataFile.retrievedToMaster = true;
            Long fileSize = (new File(localFileName + "/" + retrievedFileName).length());
            totalOutputFilesRetrieved = totalOutputFilesRetrieved + fileSize;
            //System.out.println("Retrieved file of size: " + fileSize);
        } else {
            System.out.println("File transfer failed...");
        }

    }

    public void run() {

        while (true) {
            try {
                Socket server = socket.accept();
                DataInputStream in = new DataInputStream(server.getInputStream());
                String command = in.readUTF();
                server.close();
                if (command.contains(Configuration.globalConfig.intraMasterTerminationSignal)) {
                    System.out.println("FileRetrival Thread received termination signal");
                    System.out.println("Total MB of output files received: " + (totalOutputFilesRetrieved / (1024 * 1024)));
                    break;
                } else {
                    processRequest(command);
                }
            } catch (IOException ex) {
                Logger.getLogger(FileRetriever.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        try {
            retrieveLogsFromAllSites();
            ExecutionSite.deleteDockerKey();
            HermesLogKeeper.closeLogFiles();
            Hermes.hermes.killAllResources();
        } catch (IOException ex) {
            Logger.getLogger(FileRetriever.class.getName()).log(Level.SEVERE, null, ex);
        } catch (TaskExecFailException ex) {
            Logger.getLogger(FileRetriever.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
