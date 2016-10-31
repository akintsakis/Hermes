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
import java.util.Collections;

/**
 *
 * @author thanos
 */
public class FpltDataIntensiveScheduler implements Scheduler {

    @Override
    public TreeNode scheduleNextTaskAndUpdateQueues(ArrayList<ResourceNode> availableResources, LinkedHashMap<TreeNode, String> waitingQueue) {

        if (availableResources.isEmpty()) {
            return null;
        }
        TreeNode minNode = null;
        ArrayList<TreeNode> comps = new ArrayList(waitingQueue.keySet());

        Collections.sort(comps);
        Collections.reverse(comps);

        Collections.sort(availableResources);
        Collections.reverse(availableResources);

        for (ResourceNode site : availableResources) {
            for (TreeNode node : comps) {
                if (site.availableSlots >= Integer.valueOf(node.component.threadsMin)) {
                    minNode = node;
                    break;
                }
            }
        }

        if (minNode == null) {
            return null;
        }

        double minData = Double.MAX_VALUE;
        ArrayList<ResourceNode> minTransferSites = new ArrayList<ResourceNode>();
        for (ResourceNode site : availableResources) {

            if (site.availableSlots == 0) {
                System.out.println("Fatal Error, site with 0 slots reached scheduler.. should have been cut off");
                System.exit(1);
            }

            if (site.availableSlots >= Integer.valueOf(minNode.component.threadsMin)) {
                double tempFileTransferTotal = minNode.component.calculateFileTransfersRequiredForGivenSite(site);
                if (tempFileTransferTotal < minData) {
                    minData = tempFileTransferTotal;
                    minTransferSites.clear();
                    minTransferSites.add(site);
                } else if (tempFileTransferTotal == minData) {
                    minTransferSites.add(site);
                }
            }
        }
        Collections.sort(minTransferSites);
        Collections.reverse(minTransferSites);

        minNode.component.executedOnResource = minTransferSites.get(0);

        if (minNode.component.threadsMax.equals("all")) {
            minNode.component.setAssignedThreads(minNode.component.executedOnResource.availableSlots);
        } else {
            minNode.component.setAssignedThreads(Integer.valueOf(minNode.component.threadsMin));
        }
        return minNode;

    }

}
