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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;

import net.neoremind.sshxcute.exception.TaskExecFailException;

public class Hermes {

    public static Hermes hermes;
    public HashMap<String, String> availableComponents = new HashMap<String, String>();
    public ArrayList<ResourceNode> resources;
    public HashMap<Component, TreeNode> componentToTreeNode = new HashMap<Component, TreeNode>();

    public IncomingCommunicatorThread comms;
    private FileRetriever fileRetriever;

    public WorkflowTree workflow;

    final String workflowGraphMlInput;
    final String initialInputsFolderPathOnMaster;

    private void buildResources() throws IOException, TaskExecFailException, Exception {

        ResourceNode.masterPortForwardsToSitesStartFrom = Configuration.globalConfig.masterPortForwardsToSitesStartFrom;
        resources = new ArrayList<ResourceNode>();
        ResourceNode.createDockerKeys();

        File[] resourceDesc = (new File(Configuration.globalConfig.resourcesFolder)).listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".site");
            }
        });

        final CyclicBarrier barrier = new CyclicBarrier(resourceDesc.length);
        for (int i = 0; i < resourceDesc.length; i++) {
            if (resourceDesc[i].getName().endsWith(".site")) {
                ResourceNode r = new ResourceNode(i, resourceDesc[i].getAbsolutePath(), resources, barrier);
                r.start();
                resources.add(r);
            }
        }
        for (int i = 0; i < resources.size(); i++) {
            resources.get(i).join();
        }

        if (ResourceNode.defaultResourceNode == null) {
            ResourceNode.defaultResourceNode = resources.get(0);
        }

        System.out.println("Default resource is " + ResourceNode.defaultResourceNode.name);
        Runtime.getRuntime().exec("chmod 600 " + Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers);

    }

    void killAllResources() throws TaskExecFailException, IOException {
        for (ResourceNode a : resources) {
            a.shutDownResource();
        }
    }

    private void loadComponentList(String folderPath) {
        File folder = new File(folderPath);
        File[] files = folder.listFiles();
        for (int i = 0; i < files.length; i++) {
            String componentName = files[i].getName();
            if (!componentName.endsWith("~")) {
                componentName = componentName.substring(0, componentName.indexOf("."));
                availableComponents.put(componentName, files[i].getAbsolutePath());
            }
        }
    }

    private void transferInitialInputsToDefaultResource() throws IOException {
        //System.out.println("Transfering initial inputs to default container... ");
        if (ResourceNode.defaultResourceNode.runsOnMaster) {
            for (int i = 0; i < DataFile.allDataFiles.size(); i++) {
                if (DataFile.allDataFiles.get(i).iSinitialInput) {
                    ResourceNode.defaultResourceNode.executeCommand("docker exec hermes_workflow_run mkdir -p /home/" + ResourceNode.defaultResourceNode.containerUsername + "/" + DataFile.currentFolder + "/initialInputs", false, true);
                    String extractName = (new File(DataFile.allDataFiles.get(i).pathInResource.get(DataFile.initResourceNode))).getName();
                    ResourceNode.defaultResourceNode.executeCommand("docker cp " + DataFile.allDataFiles.get(i).pathInResource.get(DataFile.initResourceNode) + " hermes_workflow_run:/home/" + ResourceNode.defaultResourceNode.containerUsername + "/" + DataFile.currentFolder + "/initialInputs/", false, true);

                    DataFile.allDataFiles.get(i).pathInResource.put(ResourceNode.defaultResourceNode, "/home/" + ResourceNode.defaultResourceNode.containerUsername + "/" + DataFile.currentFolder + "/initialInputs/" + extractName);
                    DataFile.allDataFiles.get(i).pathInResource.remove(DataFile.initResourceNode);
                }
            }

        } else {

            for (int i = 0; i < DataFile.allDataFiles.size(); i++) {
                if (DataFile.allDataFiles.get(i).iSinitialInput) {
                    ResourceNode.defaultResourceNode.executeCommand("docker exec hermes_workflow_run mkdir -p /home/" + ResourceNode.defaultResourceNode.containerUsername + "/" + DataFile.currentFolder + "/initialInputs", false, true);
                    ResourceNode.defaultResourceNode.copyFile(DataFile.allDataFiles.get(i).pathInResource.get(DataFile.initResourceNode), ResourceNode.defaultResourceNode.dockerBuildFolder);
                    String extractName = (new File(DataFile.allDataFiles.get(i).pathInResource.get(DataFile.initResourceNode))).getName();
                    ResourceNode.defaultResourceNode.executeCommand("docker cp " + ResourceNode.defaultResourceNode.dockerBuildFolder + "/" + extractName + " hermes_workflow_run:/home/" + ResourceNode.defaultResourceNode.containerUsername + "/" + DataFile.currentFolder + "/initialInputs/", false, true);
                    DataFile.allDataFiles.get(i).pathInResource.put(ResourceNode.defaultResourceNode, "/home/" + ResourceNode.defaultResourceNode.containerUsername + "/" + DataFile.currentFolder + "/initialInputs/" + extractName);
                    DataFile.allDataFiles.get(i).pathInResource.remove(DataFile.initResourceNode);
                }
            }
        }

    }

    private Hermes(String[] args) throws IOException, Exception {
        if (args.length < 2) {
            System.out.println("Either argument 1, workflow description file or argument 2, initial inputs folder not provided. Running default workflow, pangenome analysis, with sample input");
            //String locationOfHermesRootFolder = (new File(Hermes.class.getProtectionDomain().getCodeSource().getLocation().getPath())).getParentFile().getParentFile().getParentFile().toString() + "/";
            workflowGraphMlInput = Configuration.globalConfig.sampleWorkflow;
            initialInputsFolderPathOnMaster = Configuration.globalConfig.sampleInputFolder;
        } else {
            workflowGraphMlInput = args[0];
            initialInputsFolderPathOnMaster = args[1];
            if (!new File(workflowGraphMlInput).exists()) {
                System.out.println("Workflow description file does NOT exist. Did you provide the correct, ABSOLUTE path as argument 1?");
                System.exit(1);
            }

            if (!new File(initialInputsFolderPathOnMaster).exists() || !new File(initialInputsFolderPathOnMaster).isDirectory()) {
                System.out.println("Inputs folder does NOT exist. Did you provide the correct, ABSOLUTE path as argument 2?");
                System.exit(1);
            }
        }
        if (!new File(workflowGraphMlInput).exists()) {
            System.out.println("Workflow description file does NOT exist: "+workflowGraphMlInput);
            System.exit(1);
        }

        if (!new File(initialInputsFolderPathOnMaster).exists()) {
            System.out.println("Inputs folder does NOT exist: "+initialInputsFolderPathOnMaster);
            System.exit(1);
        }



        HermesLogKeeper.initializeLogs(Configuration.globalConfig.locationOfHermesRootFolder, Configuration.globalConfig.currentDate);
        DataFile.currentFolder = Configuration.globalConfig.currentDate + "_toBeDeleted";
        System.out.println(DataFile.currentFolder);

    }

    public static void main(String[] args) throws IOException, InterruptedException, Exception {

        hermes = new Hermes(args);

        hermes.loadComponentList(Configuration.globalConfig.componentDescriptions);

        System.out.println("Initializing sites...");
        hermes.buildResources();
        System.out.println("Site initialization complete!");
        
        System.out.println("Initializing Incoming Communications Thread...");
        hermes.comms = new IncomingCommunicatorThread();
        hermes.comms.start();
        
        System.out.println("Initializing File Retriever Thread...");
        hermes.fileRetriever = new FileRetriever();
        hermes.fileRetriever.start();

        System.out.println("Reading Graph to create workflow...");
        System.out.println();
        GraphmlToTreeNodes graphToTreeWorkflow = new GraphmlToTreeNodes(hermes.workflowGraphMlInput);
        graphToTreeWorkflow.createWorkflowFromGraphRecursively(graphToTreeWorkflow.graphMlRoot);
        hermes.workflow = new WorkflowTree(graphToTreeWorkflow.root, hermes.resources);
        System.out.println("Worfklow is loaded!");

        System.out.println("Transferring initial inputs to default container...");
        hermes.transferInitialInputsToDefaultResource();

        System.out.println("Workflow execution commences..");
        hermes.workflow.executeWorkflow();
        hermes.killLocalThreads();
        System.out.println("exiting...");
    }

    private void killLocalThreads() {
        talker(Configuration.globalConfig.intraMasterTerminationSignal, "localhost", Configuration.globalConfig.commsThreadListeningPortOnMaster);
        talker(Configuration.globalConfig.intraMasterTerminationSignal, "localhost", Configuration.globalConfig.fileRetrieverListeningPortOnMaster);
    }

    private void talker(String message, String serverName, Integer port) {
        try {
            Socket client = new Socket(serverName, port);
            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out
                    = new DataOutputStream(outToServer);
            out.writeUTF(message);
            out.close();
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
