/*
 * Copyright 2017 thanos.
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
package Hermes;

import java.util.ArrayList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author thanos
 */
public class HealthThread extends Thread {

    ArrayList<ExecutionSite> resources;
    public ConcurrentHashMap<String, NodeExecutionThread> executingNow;

    HealthThread(ArrayList<ExecutionSite> resources, ConcurrentHashMap<String, NodeExecutionThread> executingNow) {
        this.resources = resources;
        this.executingNow = executingNow;
    }

    public void run() {
        System.out.println("Health thread started");

        while (!WorkflowTree.workflowComplete) {
            try {
                sleep(120000);
            } catch (InterruptedException ex) {
                Logger.getLogger(HealthThread.class.getName()).log(Level.SEVERE, null, ex);
            }

            ArrayList<String> heartbeatsOk = new ArrayList<String>();
            for (ExecutionSite site : resources) {
                site.sendHeartBeat();
                heartbeatsOk.add(site.name);
            }
            System.out.print("Heartbeats ok: ");
            for (String s : heartbeatsOk) {
                System.out.print(s + " ");
            }
            System.out.println();
            System.out.print("Executing now: ");
            for (Map.Entry<String, NodeExecutionThread> entry : executingNow.entrySet()) {
                System.out.print(entry.getValue().node.component.id + "_" + entry.getValue().node.component.name + "="
                        + entry.getValue().node.component.executedOnResource.name + " ");
            }
            System.out.println();

        }
        System.out.println("Health thread exiting...");

    }

}
