/*
 * Copyright 2016 thanos.
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
 */
package Schedulers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import Hermes.Component;
import Hermes.ResourceNode;
import Hermes.TreeNode;

/**
 *
 * @author thanos
 */
public class SimpleDataIntensiveScheduler implements Scheduler {

    @Override
    public TreeNode scheduleNextTaskAndUpdateQueues(ArrayList<ResourceNode> availableResources, LinkedHashMap<TreeNode, String> waitingQueue) {

        if (availableResources.isEmpty()) {
            return null;
        }

        TreeNode minNode = null;
        double minData = Double.MAX_VALUE;
        for (ResourceNode site : availableResources) {

            if (site.availableSlots == 0) {
                System.out.println("Fatal Error, site with 0 slots reached scheduler.. should have been cut off");
                System.exit(1);
            }

            for (Map.Entry<TreeNode, String> entry : waitingQueue.entrySet()) {
                if (site.availableSlots >= Integer.valueOf(entry.getKey().component.threadsMin)) {
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
        } else {
            minNode.component.setAssignedThreads(Integer.valueOf(minNode.component.threadsMin));
        }
        return minNode;

    }

}
