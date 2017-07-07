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
import Hermes.Component;
import Hermes.ExecutionSite;
import Hermes.TreeNode;

/* The Hermes Scheduler Interface */

public interface Scheduler {

    public TreeNode scheduleNextTaskAndUpdateQueues(ArrayList<ExecutionSite> availableResources, LinkedHashMap<TreeNode, String> waitingQueue);

}
