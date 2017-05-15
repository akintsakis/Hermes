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

import Hermes.ExecutionSite;
import Hermes.TreeNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;

/**
 *
 * @author thanos
 */
public class RandomScheduler implements Scheduler {

    @Override
    public TreeNode scheduleNextTaskAndUpdateQueues(ArrayList<ExecutionSite> availableResources, LinkedHashMap<TreeNode, String> waitingQueue) {

        if (availableResources.isEmpty()) {
            return null;
        }
        TreeNode minNode = null;
        ArrayList<TreeNode> comps = new ArrayList(waitingQueue.keySet());

        for (int i = 0; i < availableResources.size() * 10; i++) {
            ExecutionSite site;
            if (availableResources.size() == 1) {
                site = availableResources.get(0);
            } else {
                site = availableResources.get((int) (Math.random() * availableResources.size()));
            }
            TreeNode node = comps.get((int) (Math.random() * comps.size()));

            Integer adjustedThreads;
            if (node.component.threadsMin.equals("blockSite")) {
                adjustedThreads = site.siteThreadCount;
            } else {
                adjustedThreads = Integer.valueOf(node.component.threadsMin);
            }
            if (site.availableSlots >= adjustedThreads) {
                minNode = node;
                break;
            }

        }

        if (minNode == null) {
            return null;
        }

        minNode.component.executedOnResource = availableResources.get(0);

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
