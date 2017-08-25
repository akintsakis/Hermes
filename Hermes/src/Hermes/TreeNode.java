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

import java.util.ArrayList;
import java.util.Map;

public class TreeNode implements Comparable<TreeNode> {

    boolean isroot = false;
    double executionStartedAtTimestamp;
    public static int globalNodeId = 0;

    String name;
    public int id;
    int level;
    ArrayList<TreeNode> children = new ArrayList<TreeNode>();
    ArrayList<TreeNode> parents;
    public Component component;
    boolean executionCompleted = false;
    long numberOfDependants;

    TreeNode(Component component, ArrayList<TreeNode> dependencies) {
        this.id = globalNodeId++;
        this.component = component;
        this.component.ofNode = this;
        for (int i = 0; i < dependencies.size(); i++) {
            dependencies.get(i).children.add(this);
        }
        parents = dependencies;
    }

    TreeNode() {
        this.id = globalNodeId++;
    }

    public void revert() {
        executionCompleted = false;
    }

    @Override
    public int compareTo(TreeNode o) {
        double thisburden = component.calculateBurdenBasedOnInputSize();
        double otherburden = o.component.calculateBurdenBasedOnInputSize();

        if (thisburden > otherburden) {
            return 1;
        } else if (thisburden < otherburden) {
            return -1;
        } else {
            return 0;
        }
    }

    public long setNumberOfDependentComponentsRecursively(ArrayList<TreeNode> children, long numOfDependants, Map<Integer, String> alreadyAdded) {

        for (TreeNode n : children) {
            if(!alreadyAdded.containsKey(n.component.id)) {
                numOfDependants++;
                alreadyAdded.put(n.component.id, "");
            }
            numOfDependants = setNumberOfDependentComponentsRecursively(n.children, numOfDependants, alreadyAdded);
        }
        return numOfDependants;
    }
}
