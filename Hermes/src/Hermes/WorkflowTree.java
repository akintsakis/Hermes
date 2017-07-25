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

import Schedulers.FirstComeScheduler;
import Schedulers.FpltDataIntensiveScheduler;
import Schedulers.FpltScheduler;
import Schedulers.RoundRobin;
import Schedulers.SimpleDataIntensiveScheduler;
import Schedulers.Scheduler;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class WorkflowTree implements Serializable {

    TreeNode root;
    double totalDataTransferAsInputFiles = 0.0;
    ArrayList<TreeNode> rootOnly = new ArrayList<TreeNode>();
    String workflowIdentifier;
    public Lock threadslock = new Lock();
    public Lock masterlock = new Lock();

    public LinkedHashMap<TreeNode, String> waitingQueue;
    public LinkedHashMap<Integer, TreeNode> allNodes = new LinkedHashMap<Integer, TreeNode>();
    HashMap<TreeNode, String> allNodesCheck = new HashMap<TreeNode, String>();

    //public ArrayList<NodeExecutionThread> actuallyExecutingQueue = new ArrayList<NodeExecutionThread>();
    public ArrayList<NodeExecutionThread> executionComplete = new ArrayList<NodeExecutionThread>();

    public ArrayList<ExecutionSite> resources;
    public ArrayList<ExecutionSite> availableResources = new ArrayList<ExecutionSite>();

    WorkflowTree(TreeNode root, ArrayList<ExecutionSite> resources) throws IOException {
        this.resources = resources;
        this.root = root;
        root.executionCompleted = true;
        getAllNodes(root);
        allNodesCheck = null;
        workflowIdentifier = new Date().toString();
    }

    public void getAllNodes(TreeNode a) {
        if (!allNodesCheck.containsKey(a) && a.id != 0) {
            allNodesCheck.put(a, "");
            allNodes.put(a.id, a);
        }
        for (int i = 0; i < a.children.size(); i++) {
            getAllNodes(a.children.get(i));
        }
    }

    public void executeWorkflow() throws InterruptedException, IOException {
        revertWorkflow();
        long startTime = System.currentTimeMillis();

        for (ExecutionSite node : resources) {
            availableResources.add(node);
        }
        //actuallyExecutingQueue.clear();
        executionComplete.clear();

        double currentTime = 0.0;
        waitingQueue = new LinkedHashMap<TreeNode, String>();
        LinkedHashMap<TreeNode, String> blockedQueue = new LinkedHashMap<TreeNode, String>();
        ArrayList<TreeNode> executingQueue = new ArrayList<TreeNode>();
        addNodeChildrenToWaitingQueueOrBlockedQueue(root, waitingQueue, blockedQueue);
        System.out.println();

        do {
            scheduleNodesFromWaitingQueueRealExecution(waitingQueue, executingQueue, currentTime);

            TreeNode justFinished = getFirstNodeToCompleteFromExecutingQueueRealExecution(executingQueue);
            if (justFinished != null) {

                double tmpTime = justFinished.executionStartedAtTimestamp + justFinished.component.runtimeInMs;
                if (currentTime < tmpTime) {
                    currentTime = tmpTime;
                }
                if (!justFinished.component.lastExecutionFailed) {
                    addNodeChildrenToWaitingQueueOrBlockedQueue(justFinished, waitingQueue, blockedQueue);
                } else {
                    System.out.println("Putting back to waiting queue: " + justFinished.component.name);
                    justFinished.executionCompleted = false;
                    justFinished.component.executionCompleted = false;
                    justFinished.component.clearComponentOutputPathsAfterFailedRun();
                    justFinished.component.restoreOriginalCommandForRerun();
                    waitingQueue.put(justFinished, "");
                }

            }

            System.out.println("Waiting Queue: " + waitingQueue.size() + " Executing Queue: " + executingQueue.size() + " Blocked Queue: " + blockedQueue.size());
            System.out.print("In use: ");
            for (TreeNode n : executingQueue) {
                System.out.print(n.component.executedOnResource.name + ",");
            }
            System.out.print("\n");

        } while (waitingQueue.size() > 0 || executingQueue.size() > 0 || blockedQueue.size() > 0);
        System.out.println("All done, master finished");
        System.out.println("Total data transferred: " + (totalDataTransferAsInputFiles / (1024.0 * 1024.0)) + " MB");
        System.out.println("Workflow Duration: " + ((System.currentTimeMillis() - startTime) / 1024) + " s");

    }

    public TreeNode getFirstNodeToCompleteFromExecutingQueueRealExecution(ArrayList<TreeNode> executingQueue) throws InterruptedException {
        if (executionComplete.isEmpty()) {
            //  System.out.println("master waiting for something to finish");
            masterlock.waitFor();
            // System.out.println("master notified");

            for (int i = 0; i < executingQueue.size(); i++) {
                if (executingQueue.get(i) == executionComplete.get(0).node) {
                    executingQueue.remove(i);
                    break;
                }
            }
            TreeNode toReturn = executionComplete.get(0).node;
            executionComplete.remove(0);
            toReturn.executionCompleted = true;
            toReturn.component.executionCompleted = true;
            toReturn.component.executedOnResource.componentsCurrentlyExecutingOnSites.remove(toReturn.component);
            if (!availableResources.contains(toReturn.component.executedOnResource)) {
                availableResources.add(toReturn.component.executedOnResource);
            }
            toReturn.component.executedOnResource.availableSlots = toReturn.component.executedOnResource.availableSlots + toReturn.component.threadsAssigned;
            return toReturn;
        } else {
            for (int i = 0; i < executingQueue.size(); i++) {
                if (executingQueue.get(i) == executionComplete.get(0).node) {
                    executingQueue.remove(i);
                    break;
                }
            }
            TreeNode toReturn = executionComplete.get(0).node;
            executionComplete.remove(0);
            toReturn.executionCompleted = true;
            toReturn.component.executedOnResource.componentsCurrentlyExecutingOnSites.remove(toReturn.component);
            if (!availableResources.contains(toReturn.component.executedOnResource)) {
                availableResources.add(toReturn.component.executedOnResource);

            }
            toReturn.component.executedOnResource.availableSlots = toReturn.component.executedOnResource.availableSlots + toReturn.component.threadsAssigned;
            return toReturn;
        }
    }

    public void scheduleNodesFromWaitingQueueRealExecution(LinkedHashMap<TreeNode, String> waitingQueue, ArrayList<TreeNode> executingQueue, double currentTime) throws IOException {
        LinkedHashMap<TreeNode, String> scheduledAndMarkedForRemoval = new LinkedHashMap<TreeNode, String>();

        while (!waitingQueue.isEmpty()) {
            Scheduler scheduler = selectScheduler();
            //Scheduler scheduler = new FpltDataIntensiveScheduler();            
            //Scheduler scheduler = new SimpleDataIntensiveScheduler();
            //Scheduler scheduler = new FpltScheduler();
            //Scheduler scheduler = new FirstComeScheduler();

            TreeNode toBeExecuted = scheduler.scheduleNextTaskAndUpdateQueues(availableResources, waitingQueue);

            if (toBeExecuted == null) {
                //System.out.println("None of the available sites have enough slots to execute any of the waiting components... Waiting for sites or slots to be freed.");
                break;
            }

            waitingQueue.remove(toBeExecuted);
            toBeExecuted.component.executedOnResource.availableSlots = toBeExecuted.component.executedOnResource.availableSlots - toBeExecuted.component.threadsAssigned;
            if (toBeExecuted.component.executedOnResource.availableSlots < 0) {
                System.out.println("Fatal error, site slots are less than zero... :" + toBeExecuted.component.executedOnResource.availableSlots);
                System.exit(1);
            }
            executingQueue.add(toBeExecuted);
            toBeExecuted.component.executedOnResource.componentsCurrentlyExecutingOnSites.add(toBeExecuted.component);
            toBeExecuted.executionStartedAtTimestamp = currentTime;
            NodeExecutionThread exec = new NodeExecutionThread(toBeExecuted, this);
            Hermes.hermes.comms.executingNow.put(String.valueOf(exec.id), exec);

            toBeExecuted.component.dataTransferredAsInputFiles = toBeExecuted.component.calculateFileTransfersRequiredForGivenSite(toBeExecuted.component.executedOnResource);
            totalDataTransferAsInputFiles = totalDataTransferAsInputFiles + toBeExecuted.component.dataTransferredAsInputFiles;

            if (toBeExecuted.component.executedOnResource.availableSlots == 0) {
                //System.out.println("Removing site: " + toBeExecuted.component.executedOnResource.name);
                availableResources.remove(toBeExecuted.component.executedOnResource);
            }
            exec.start();
        }
    }

    public void addNodeChildrenToWaitingQueueOrBlockedQueue(TreeNode a, LinkedHashMap<TreeNode, String> waitingQueue, LinkedHashMap<TreeNode, String> blockedQueue) {
        //System.out.println("children size: "+a.children.size());
        for (int i = 0; i < a.children.size(); i++) {
            TreeNode currentNode = a.children.get(i);
            //System.out.println(currentNode.component.name);
            if (checkNodeDependencies(a.children.get(i)) && !waitingQueue.containsKey(currentNode)) {
                // System.out.println("adding to waiting queue: "+currentNode.component.name);
                waitingQueue.put(currentNode, "");
                if (blockedQueue.containsKey(currentNode)) {
                    // System.out.println("removing from blocked queue: "+currentNode.component.name);
                    blockedQueue.remove(currentNode);
                }
            } else if (!blockedQueue.containsKey(currentNode)) {
                // System.out.println("adding to blocked queue: "+currentNode.component.name);
                blockedQueue.put(currentNode, "");
            }
        }
    }

    public boolean checkNodeDependencies(TreeNode a) {
        //System.out.println("PARENTS SIZE IS: " + a.parents.size());
        for (int i = 0; i < a.parents.size(); i++) {
            //System.out.println("CH " + a.parents.get(i).executionCompleted);
            if (false == a.parents.get(i).executionCompleted) {
                //System.out.println("CH "+a.parents.get(i).executionCompleted);
                return false;
            }
        }
        //System.out.println("Node is clear to run: " + a.id);
        return true;
    }

    public boolean availableSlotsExistOnResource(Component component, ExecutionSite resource) {
        if (component.slots >= resource.availableSlots) {
            return true;
        }
        return false;
    }

    public void revertWorkflow() {
        //Revert resource Node status
        for (ExecutionSite resource : resources) {
            resource.revert();
        }
        for (Map.Entry<Integer, TreeNode> entry : allNodes.entrySet()) {
            entry.getValue().revert();
        }
    }

    public Scheduler selectScheduler() {
        Scheduler scheduler = null;
        if (Configuration.globalConfig.scheduler.toLowerCase().equals("roundrobin")) {
            scheduler = new RoundRobin();
        }
        else if (Configuration.globalConfig.scheduler.toLowerCase().equals("fpltdataintensivescheduler")) {
            scheduler = new FpltDataIntensiveScheduler();
        }
        else if (Configuration.globalConfig.scheduler.toLowerCase().equals("fpltscheduler")) {
            scheduler = new FpltScheduler();
        }
        else if (Configuration.globalConfig.scheduler.toLowerCase().equals("simpledataintensivescheduler")) {
            scheduler = new SimpleDataIntensiveScheduler();
        } else {
            System.out.println("Fatal error, non implemented scheduler selected. Exiting...");
            System.exit(1);
        }

        return scheduler;
    }
}
