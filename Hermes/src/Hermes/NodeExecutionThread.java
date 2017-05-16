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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class NodeExecutionThread extends Thread {

    TreeNode node;
    int id;
    WorkflowTree workflow;
    public static int NodeExecutionThreadIdCounter = 0;
    Lock NodeExecutionThreadQueue = new Lock();

    NodeExecutionThread(TreeNode node, WorkflowTree workflow) throws IOException {
        this.node = node;
        this.workflow = workflow;
        this.id = NodeExecutionThreadIdCounter++;
    }

    public void run() {
        try {
            System.out.println();
            String initMessage = (new Date()).toString() + " - Component :" + node.component.id + "_" + node.component.name + " execution commenced on site:" + node.component.executedOnResource.name + " slots: " + node.component.threadsAssigned;
            System.out.println(initMessage);
            HermesLogKeeper.logSender(initMessage);

            node.component.setComponentOutputPaths();
            satisfyInputDependencies();
            node.component.initComponentOnResourceNode();

            initializeComponentWorkingDirectory();
            executeComponent();

        } catch (InterruptedException ex) {
            Logger.getLogger(NodeExecutionThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (TaskExecFailException ex) {
            Logger.getLogger(NodeExecutionThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void initializeComponentWorkingDirectory() throws TaskExecFailException, InterruptedException {
        String currentCommand = "sh;-c;mkdir -p " + DataFile.currentFolder + "/" + node.component.folderId + "/";
        JSONObject jsonCommand = new JSONObject();
        jsonCommand.put("command", currentCommand);
        jsonCommand.put("NodeExecutionThreadId", String.valueOf(id));
        //sendIt(jsonCommand.toJSONString(), node.component.executedOnResource.tunnelIP, node.component.executedOnResource.masterTunnelPortCommunications);
        sleep(300);
        node.component.executedOnResource.passCommandToClientWithinContainer(jsonCommand.toJSONString(), node.component, 3);
        NodeExecutionThreadQueue.waitFor();

    }

    public void satisfyInputDependencies() throws TaskExecFailException, InterruptedException {
        for (DataFile file : node.component.inputDataFiles) {
            sleep(300);
            file.resourceLocks.get(node.component.executedOnResource).lock();
            if (file.iSinitialInput && !file.pathInResource.containsKey(node.component.executedOnResource)) {
                transferFile(ExecutionSite.defaultResourceNode, file, "initialInputs");
            } else if (!file.pathInResource.containsKey(node.component.executedOnResource)) {
                String dest = String.valueOf(file.createdByComponent.id) + file.createdByComponent.name + "_transfered";
                transferFile(file.pathInResource.entrySet().iterator().next().getKey(), file, dest);
            }
            file.resourceLocks.get(node.component.executedOnResource).unlock();
        }
    }

    public void transferFile(ExecutionSite from, DataFile file, String componentFolder) throws InterruptedException {
        String currentCommand = "sh;-c;mkdir -p " + DataFile.currentFolder + "/" + componentFolder + "/";
        JSONObject jsonCommandMakeDir = new JSONObject();
        jsonCommandMakeDir.put("command", currentCommand);
        jsonCommandMakeDir.put("NodeExecutionThreadId", String.valueOf(id));

        node.component.executedOnResource.passCommandToClientWithinContainer(jsonCommandMakeDir.toJSONString(), node.component, 3);
        NodeExecutionThreadQueue.waitFor();
        if (file.pathInResource.get(from).contains("makeBlastProteinDB")) {
            Path p = Paths.get(file.pathInResource.get(from));
            p.getFileName().toString();
            Path p1 = Paths.get(file.pathInResource.get(from));
            String destinationPath = node.component.executedOnResource.containerUserHomePath + "/" + DataFile.currentFolder + "/" + componentFolder + "/" + p1.getParent().getFileName();
            String port = ExecutionSite.portMappings.get(String.valueOf(node.component.executedOnResource.id) + "_" + String.valueOf(from.id));
            String command = "sh;-c;scp -c arcfour -q -r -C -P " + port + " " + Configuration.globalConfig.containerUsernameForSSH + "@" + from.forwardedFileTransfersHostname + ":" + p1.getParent() + " " + destinationPath + " && du -h " + destinationPath + " && ls -la " + destinationPath;
            JSONObject jsonCommandFileTransfer = new JSONObject();
            jsonCommandFileTransfer.put("command", command);
            jsonCommandFileTransfer.put("NodeExecutionThreadId", String.valueOf(id));
            node.component.executedOnResource.passCommandToClientWithinContainer(jsonCommandFileTransfer.toJSONString(), node.component, 3);
            NodeExecutionThreadQueue.waitFor();
            file.pathInResource.put(node.component.executedOnResource, destinationPath + "/" + p1.getFileName());
        } else {
            Path p = Paths.get(file.pathInResource.get(from));
            p.getFileName().toString();
            String destinationPath = node.component.executedOnResource.containerUserHomePath + "/" + DataFile.currentFolder + "/" + componentFolder + "/" + p.getFileName().toString();
            String port = ExecutionSite.portMappings.get(String.valueOf(node.component.executedOnResource.id) + "_" + String.valueOf(from.id));
            String command = "sh;-c;scp -c arcfour -q -r -C -P " + port + " " + Configuration.globalConfig.containerUsernameForSSH + "@" + from.forwardedFileTransfersHostname + ":" + file.pathInResource.get(from) + " " + destinationPath + " && du -h " + destinationPath + " && ls -la " + destinationPath;
            JSONObject jsonCommandFileTransfer = new JSONObject();
            jsonCommandFileTransfer.put("command", command);
            jsonCommandFileTransfer.put("NodeExecutionThreadId", String.valueOf(id));
            node.component.executedOnResource.passCommandToClientWithinContainer(jsonCommandFileTransfer.toJSONString(), node.component, 3);

            NodeExecutionThreadQueue.waitFor();
            file.pathInResource.put(node.component.executedOnResource, destinationPath);
        }
    }

    public void executeComponent() throws InterruptedException {
        sleep(300);
        if (!node.component.command.startsWith("sh;-c;")) {
            node.component.command = "sh;-c;" + node.component.command;
            node.component.command = "sh;-c;" + node.component.command.substring(6, node.component.command.length()).replace(";", " ");
        }

        StringBuilder bytesSB = new StringBuilder();
        StringBuilder customSB = new StringBuilder();
        for (int i = 0; i < node.component.inputDataFiles.size(); i++) {
            bytesSB.append(String.valueOf(node.component.inputDataFiles.get(i).realFileSizeInB)).append(" ");
        }
        for (int i = 0; i < node.component.inputDataFiles.size(); i++) {
            customSB.append(String.valueOf(node.component.inputDataFiles.get(i).realFileSizeCustom)).append(" ");
        }

        JSONObject jsonCommand = new JSONObject();
        jsonCommand.put("componentID", String.valueOf(node.component.id));
        jsonCommand.put("NodeID", String.valueOf(node.id));
        jsonCommand.put("componentName", String.valueOf(node.component.name));
        jsonCommand.put("monitor", true);
        jsonCommand.put("timeout", Configuration.timeOut);
        jsonCommand.put("inputOutputFileAssessment", Configuration.globalConfig.inputOutputFileAssessment);
        jsonCommand.put("command", node.component.command);
        jsonCommand.put("slotsUsed", node.component.slots);
        jsonCommand.put("resourceName", node.component.executedOnResource.name);
        JSONArray outputDataFilesList = new JSONArray();
        for (int i = 0; i < node.component.outputDataFiles.size(); i++) {
            JSONObject datafile = new JSONObject();
            datafile.put("ID", String.valueOf(node.component.outputDataFiles.get(i).id));
            datafile.put("path", node.component.outputDataFiles.get(i).pathInResource.get(node.component.executedOnResource));
            outputDataFilesList.add(datafile);
        }

        JSONArray inputDataFilesList = new JSONArray();
        for (int i = 0; i < node.component.inputDataFiles.size(); i++) {
            JSONObject datafile = new JSONObject();
            datafile.put("ID", String.valueOf(node.component.inputDataFiles.get(i).id));
            datafile.put("path", node.component.inputDataFiles.get(i).pathInResource.get(node.component.executedOnResource));
            inputDataFilesList.add(datafile);
        }
        jsonCommand.put("OutputDataFiles", outputDataFilesList);
        jsonCommand.put("InputDataFiles", inputDataFilesList);
        jsonCommand.put("inputsRealFileSizesInB", bytesSB.toString());
        jsonCommand.put("inputsRealFileSizesCustom", customSB.toString());
        jsonCommand.put("NodeExecutionThreadId", String.valueOf(id));
        jsonCommand.put("NodeExecutionThreadFinalized", true);
        if (node.component.runningWithNumOfThreads != null) {
            jsonCommand.put("threadsAssigned", node.component.runningWithNumOfThreads);
        }
        jsonCommand.put("totalSystemCPUs", node.component.executedOnResource.siteThreadCount);
        jsonCommand.put("systemRAMSizeInMB", String.format("%.1f", node.component.executedOnResource.ramSizeInMB));
        jsonCommand.put("cpuSingleThreadedBenchmark", String.format("%.5f", node.component.executedOnResource.cpuSingleThreadedScore));
        jsonCommand.put("cpuMultiThreadedBenchmark", String.format("%.5f", node.component.executedOnResource.cpuMultithreadedScore));
        node.component.executedOnResource.passCommandToClientWithinContainer(jsonCommand.toJSONString(), node.component, 3);
        NodeExecutionThreadQueue.waitFor();
    }
}
