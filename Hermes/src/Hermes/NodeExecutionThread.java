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
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.neoremind.sshxcute.exception.TaskExecFailException;

public class NodeExecutionThread extends Thread {

    Gson gson = new Gson();
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

    public void initializeComponentWorkingDirectory() throws InterruptedException {
        String currentCommand = "sh;-c;mkdir -p " + DataFile.currentFolder + "/" + node.component.folderId + "/";
        JobRequest jobRequest = new JobRequest();
        jobRequest.command = currentCommand;
        jobRequest.NodeExecutionThreadId = String.valueOf(id);
        //sendIt(jsonCommand.toJSONString(), node.component.executedOnResource.tunnelIP, node.component.executedOnResource.masterTunnelPortCommunications);
        sleep(300);
        node.component.executedOnResource.passCommandToClientWithinContainer(gson.toJson(jobRequest), node.component, 3);
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
        JobRequest jobRequestMakeDir = new JobRequest();
        jobRequestMakeDir.command = currentCommand;
        jobRequestMakeDir.NodeExecutionThreadId = String.valueOf(id);

        node.component.executedOnResource.passCommandToClientWithinContainer(gson.toJson(jobRequestMakeDir), node.component, 3);
        NodeExecutionThreadQueue.waitFor();
        if (file.pathInResource.get(from).contains("makeBlastProteinDB")) {
            Path p = Paths.get(file.pathInResource.get(from));
            p.getFileName().toString();
            Path p1 = Paths.get(file.pathInResource.get(from));
            String destinationPath = node.component.executedOnResource.containerUserHomePath + "/" + DataFile.currentFolder + "/" + componentFolder + "/" + p1.getParent().getFileName();
            String port = ExecutionSite.portMappings.get(String.valueOf(node.component.executedOnResource.id) + "_" + String.valueOf(from.id));
            String command = "sh;-c;scp -c arcfour -q -r -C -P " + port + " " + Configuration.globalConfig.containerUsernameForSSH + "@" + from.forwardedFileTransfersHostname + ":" + p1.getParent() + " " + destinationPath + " && du -h " + destinationPath + " && ls -la " + destinationPath;
            JobRequest jobRequest = new JobRequest();
            jobRequest.command = command;
            jobRequest.jobIsFileTransfer = true;
            jobRequest.receivingFile = destinationPath;
            jobRequest.NodeExecutionThreadId = String.valueOf(id);
            node.component.executedOnResource.passCommandToClientWithinContainer(gson.toJson(jobRequest), node.component, 3);
            NodeExecutionThreadQueue.waitFor();
            file.pathInResource.put(node.component.executedOnResource, destinationPath + "/" + p1.getFileName());
        } else {
            Path p = Paths.get(file.pathInResource.get(from));
            p.getFileName().toString();
            String destinationPath = node.component.executedOnResource.containerUserHomePath + "/" + DataFile.currentFolder + "/" + componentFolder + "/" + p.getFileName().toString();
            String port = ExecutionSite.portMappings.get(String.valueOf(node.component.executedOnResource.id) + "_" + String.valueOf(from.id));
            String command = "sh;-c;scp -c arcfour -q -r -C -P " + port + " " + Configuration.globalConfig.containerUsernameForSSH + "@" + from.forwardedFileTransfersHostname + ":" + file.pathInResource.get(from) + " " + destinationPath + " && du -h " + destinationPath + " && ls -la " + destinationPath;

            JobRequest jobRequestFileTransfer = new JobRequest();
            jobRequestFileTransfer.command = command;
            jobRequestFileTransfer.NodeExecutionThreadId = String.valueOf(id);
            jobRequestFileTransfer.jobIsFileTransfer = true;
            jobRequestFileTransfer.receivingFile = destinationPath;
            node.component.executedOnResource.passCommandToClientWithinContainer(gson.toJson(jobRequestFileTransfer), node.component, 3);

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

//        StringBuilder bytesSB = new StringBuilder();
//        StringBuilder customSB = new StringBuilder();
//        for (int i = 0; i < node.component.inputDataFiles.size(); i++) {
//            bytesSB.append(String.valueOf(node.component.inputDataFiles.get(i).realFileSizeInB)).append(" ");
//        }
//        for (int i = 0; i < node.component.inputDataFiles.size(); i++) {
//            customSB.append(String.valueOf(node.component.inputDataFiles.get(i).realFileSizeCustom)).append(" ");
//        }

        //JSONObject jsonCommand = new JSONObject();
        JobRequest jobRequest = new JobRequest();
        jobRequest.componentID = String.valueOf(node.component.id);
        jobRequest.NodeID = String.valueOf(node.id);
        jobRequest.componentName = String.valueOf(node.component.name);
        jobRequest.monitor = true;
        jobRequest.timeout = Configuration.timeOut;
        jobRequest.inputOutputFileAssessment = Configuration.globalConfig.inputOutputFileAssessment;
        jobRequest.command = node.component.command;
        jobRequest.slotsUsed = node.component.slots;
        jobRequest.resourceName = node.component.executedOnResource.name;

        for (int i = 0; i < node.component.outputDataFiles.size(); i++) {
            jobRequest.outputDataFileIds.add(node.component.outputDataFiles.get(i).id);
            jobRequest.outputDataFilePaths.add(node.component.outputDataFiles.get(i).pathInResource.get(node.component.executedOnResource));
        }

        for (int i = 0; i < node.component.inputDataFiles.size(); i++) {
            jobRequest.inputDataFileIds.add(node.component.inputDataFiles.get(i).id);
            //jobRequest.inputDataFilePaths.add(node.component.inputDataFiles.get(i).pathInResource.get(node.component.executedOnResource));
            jobRequest.jobInputFileMetrics.put(String.valueOf(node.component.inputDataFiles.get(i).id), node.component.inputDataFiles.get(i).metrics);
        }

        //jobRequest.inputsRealFileSizesInB = bytesSB.toString();
        //jobRequest.inputsRealFileSizesCustom = customSB.toString();
        jobRequest.NodeExecutionThreadId = String.valueOf(id);
        jobRequest.NodeExecutionThreadFinalized = true;
        if (node.component.runningWithNumOfThreads != null) {
            jobRequest.threadsAssigned = node.component.runningWithNumOfThreads;
        }

        jobRequest.threadsUsedByComponent = node.component.threadsAssigned;
        jobRequest.totalSystemCPUs = node.component.executedOnResource.siteThreadCount;
        jobRequest.systemRAMSizeInMB = String.format("%.1f", node.component.executedOnResource.ramSizeInMB);

        jobRequest.cpuSingleThreadedBenchmark = String.format("%.5f", node.component.executedOnResource.cpuSingleThreadedScore);
        jobRequest.cpuMultiThreadedBenchmark = String.format("%.5f", node.component.executedOnResource.cpuMultithreadedScore);

        node.component.executedOnResource.passCommandToClientWithinContainer(gson.toJson(jobRequest), node.component, 3);
        NodeExecutionThreadQueue.waitFor();
    }
}
