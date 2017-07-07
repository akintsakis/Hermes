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
package Schedulers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import Hermes.Component;
import Hermes.ExecutionSite;
import Hermes.TreeNode;


/* This class implements the Simple Data Intensive Scheduler */

public class SimpleDataIntensiveScheduler implements Scheduler {

    @Override
    public TreeNode scheduleNextTaskAndUpdateQueues(ArrayList<ExecutionSite> availableResources, LinkedHashMap<TreeNode, String> waitingQueue) {

        if (availableResources.isEmpty()) {
            return null;
        }

        TreeNode minNode = null;
        double minData = Double.MAX_VALUE;
        for (ExecutionSite site : availableResources) {

            if (site.availableSlots == 0) {
                System.out.println("Fatal Error, site with 0 slots reached scheduler.. should have been cut off");
                System.exit(1);
            }

            for (Map.Entry<TreeNode, String> entry : waitingQueue.entrySet()) {
                Integer adjustedThreads;
                if (entry.getKey().component.threadsMin.equals("blockSite")) {
                    adjustedThreads = site.siteThreadCount;
                } else {
                    adjustedThreads = Integer.valueOf(entry.getKey().component.threadsMin);
                }
                if (site.availableSlots >= adjustedThreads) {
                    double tempFileTransferTotal = entry.getKey().component.calculateFileTransfersRequiredForGivenSite(site);
                    if (tempFileTransferTotal < minData) {
                        minData = tempFileTransferTotal;
                        minNode = entry.getKey();
                        minNode.component.executedOnResource = site;
                    }
                }
            }
        }
        if (minNode == null) {
            return null;
        }
        if (minNode.component.threadsMax.equals("all")) {
            minNode.component.setAssignedThreads(minNode.component.executedOnResource.availableSlots);
        } else if (Integer.valueOf(minNode.component.threadsMax) >= minNode.component.executedOnResource.availableSlots) {
            minNode.component.setAssignedThreads(minNode.component.executedOnResource.availableSlots);
        } else {
            minNode.component.setAssignedThreads(Integer.valueOf(minNode.component.threadsMax));
        }
        return minNode;

    }

}
