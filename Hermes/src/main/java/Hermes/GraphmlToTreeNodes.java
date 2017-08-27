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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

public class GraphmlToTreeNodes {

    Graph graph;
    GraphMLReader reader;
    Vertex graphMlRoot;
    TreeNode root;

    public static HashMap<Vertex, ArrayList<DataFile>> verticestoData = new HashMap<Vertex, ArrayList<DataFile>>();
    public static HashMap<Vertex, String> alreadyVisited = new HashMap<Vertex, String>();
    public static ArrayList<TreeNode> keepTreeNodes = new ArrayList<TreeNode>();

    public ArrayList<Vertex> orderToVisit = new ArrayList<Vertex>();

    public static HashMap<Vertex, String> storeVisited = new HashMap<Vertex, String>();

    GraphmlToTreeNodes(String graphmlFile) throws FileNotFoundException, IOException {
        graph = new TinkerGraph();
        reader = new GraphMLReader(graph);

        InputStream is = new BufferedInputStream(new FileInputStream(graphmlFile));
        reader.inputGraph(is);
        root = new TreeNode();
        graphMlRoot = findRoot(graph);
    }

    public String getFileType(HashMap<String, String> vertexArgs) {
        if (vertexArgs.containsKey("fileType")) {
            return vertexArgs.get("fileType");
        } else {
            return "unknown";
        }
    }

    public String getThreadsMin(HashMap<String, String> vertexArgs) {
        if (vertexArgs.containsKey("threadsMin")) {
            return vertexArgs.get("threadsMin");
        } else {
            return "blockSite";
        }
    }

    public String getThreadsMax(HashMap<String, String> vertexArgs) {
        if (vertexArgs.containsKey("threadsMax")) {
            return vertexArgs.get("threadsMax");
        } else {
            return "all";
        }
    }

    public String getExpType(HashMap<String, String> vertexArgs) {
        if (vertexArgs.containsKey("exp")) {
            return vertexArgs.get("exp");
        } else {
            return "input1";
        }
    }

    public ArrayList<Vertex> findVertexChildren(Vertex currentVertex) {
        Iterable<Edge> currentVertexEdges = currentVertex.getEdges(Direction.OUT, "_default");
        Iterator<Edge> edgesIterator = currentVertexEdges.iterator();
        ArrayList<Vertex> children = new ArrayList<Vertex>();
        while (edgesIterator.hasNext()) {
            Edge edge = edgesIterator.next();
            //System.out.println((String) edge.getProperty("description"));
            //System.out.println(edge.getLabel());            
            Vertex currentChild = edge.getVertex(Direction.IN);
            children.add(currentChild);
            //System.out.println((String) currentChild.getProperty("description"));
        }
        return children;
    }

    public ArrayList<Vertex> findVertexParents(Vertex currentVertex) {
        Iterable<Edge> currentVertexEdges = currentVertex.getEdges(Direction.IN, "_default");
        Iterator<Edge> edgesIterator = currentVertexEdges.iterator();
        ArrayList<Vertex> parents = new ArrayList<Vertex>();
        while (edgesIterator.hasNext()) {
            Edge edge = edgesIterator.next();
            //System.out.println((String) edge.getProperty("description"));
            //System.out.println(edge.getLabel());

            Vertex currentChild = edge.getVertex(Direction.OUT);
            parents.add(currentChild);
            //System.out.println((String) currentChild.getProperty("description"));
        }
        return parents;
    }

    public Vertex findRoot(Graph graph) {
        Iterable<Vertex> vertices = graph.getVertices();
        Iterator<Vertex> verticesIterator = vertices.iterator();
        while (verticesIterator.hasNext()) {
            Vertex vertex = verticesIterator.next();
            String description = (String) vertex.getProperty("description");
            if (description != null && description.contains("type=start")) {
                return vertex;
            }
        }
        return null;
    }

    public void determineVisitOrder(Vertex currentRoot) {
        ArrayList<Vertex> children = findVertexChildren(currentRoot);
    }

    public void createWorkflowFromGraphRecursively(Vertex currentRoot) throws ParserConfigurationException, SAXException, IOException {

        ArrayList<Vertex> children = findVertexChildren(currentRoot);
        for (int i = 0; i < children.size(); i++) {
            Vertex currentChild = children.get(i);
            String currentChildDescription = (String) currentChild.getProperty("description");
            //System.out.println("CurrentChildDescription: " + currentChildDescription);
            HashMap<String, String> vertexArgs = parseVertexArgs(currentChildDescription);

            if (!vertexArgs.get("type").equals("Component")) {
                if (!alreadyVisited.containsKey(currentChild)) {
                    alreadyVisited.put(currentChild, "");
                    createWorkflowFromGraphRecursively(currentChild);
                }
            } else {
                ArrayList<Vertex> parents = findVertexParents(currentChild);
                boolean ready = true;
                for (int j = 0; j < parents.size(); j++) {
                    if (!alreadyVisited.containsKey(parents.get(j))) {
                        ready = false;
                    }
                }
                if (ready && !alreadyVisited.containsKey(currentChild)) {
                    alreadyVisited.put(currentChild, "");
                    if (Hermes.hermes.availableComponents.containsKey(vertexArgs.get("name"))) {
                        buildComponentsFromVertex(currentChild);
                    } else {
                        System.out.println("INVALID COMPONENT TYPE, " + vertexArgs.get("name") + " not contained in available components list");
                        System.exit(1);
                    }
                    createWorkflowFromGraphRecursively(currentChild);
                }
            }
        }
    }

    public TreeMap<Integer, ArrayList<DataFile>> addInputsToVertexToData(ArrayList<Vertex> inputVertexes, Vertex componentVertex) throws IOException {

        TreeMap<Integer, ArrayList<DataFile>> currentComponentInputsOrdered = new TreeMap<Integer, ArrayList<DataFile>>();

        for (int i = 0; i < inputVertexes.size(); i++) {
            Vertex currentVertex = inputVertexes.get(i);
            String inputID = "invalid";
            Iterable<Edge> currentVertexEdges = currentVertex.getEdges(Direction.OUT, "_default");
            Iterator<Edge> edgesIterator = currentVertexEdges.iterator();
            while (edgesIterator.hasNext()) {
                Edge edge = edgesIterator.next();
                Vertex currentChild = edge.getVertex(Direction.IN);
                if (currentChild == componentVertex) {
                    HashMap<String, String> vertexArgs = parseVertexArgs((String) edge.getProperty("description"));
                    inputID = vertexArgs.get("inputID");
                    //System.out.println("!!!!INPUT ID = " + inputID);
                }
            }

            String currentVertexDescription = (String) currentVertex.getProperty("description");
            HashMap<String, String> vertexArgs = parseVertexArgs(currentVertexDescription);
            if (!(vertexArgs.get("type").equals("DataFile") || vertexArgs.get("type").equals("DataFile[]"))) {
                System.out.println("Fatal Error: Component has input Vertex with type not equal to DataFile or DataFile[], found: " + vertexArgs.get("type"));
                System.exit(1);
            } else {
                if (!verticestoData.containsKey(currentVertex)) {
                    ArrayList<DataFile> in = new ArrayList<DataFile>();
                    if (vertexArgs.get("type").equals("DataFile")) {
                        String relativePath = vertexArgs.get("path");
                        String fileType = getFileType(vertexArgs);
                        boolean isInitialInput = false;
                        if (vertexArgs.containsKey("initialInput") && vertexArgs.get("initialInput").equals("true")) {
                            isInitialInput = true;
                            relativePath = relativePath.replace(Configuration.globalConfig.initialInputsGraphMLAlias, Hermes.hermes.initialInputsFolderPathOnMaster);
                        }
                        boolean isFinalOutputBool = false;
                        boolean isDir = false;

                        if (vertexArgs.containsKey("finalOutput")) {
                            String isFinalOutput = vertexArgs.get("finalOutput");
                            if (isFinalOutput.equals("true")) {
                                isFinalOutputBool = true;
                            }
                        }
                        if (vertexArgs.containsKey("isDir")) {
                            String isDirString = vertexArgs.get("isDir");
                            if (isDirString.equals("true")) {
                                isDir = true;
                            }
                        }

                        DataFile file = new DataFile(relativePath, fileType, isInitialInput, isFinalOutputBool, isDir);
                        if (file.iSinitialInput) {
                            File f = new File(file.pathInResource.get(DataFile.initResourceNode));
                            if (!f.exists()) {
                                System.out.println("FATAL ERROR, input file does not exist on master");
                                System.out.println(f.getAbsolutePath());
                                System.exit(1);
                            }
                            DataFileEvaluationFunctions.selector(f.getAbsolutePath(), file.metrics, Configuration.globalConfig.inputOutputFileAssessment, null);

//                            if (file.realFileSizeInB == 0.0) {
//                                System.out.println("FATAL ERROR, initial input size and custom size values not set");
//                                System.exit(1);
//                            }
                        }
                        in.add(file);
                    } else if (vertexArgs.get("type").equals("DataFile[]")) {
                        boolean isInitialInput = false;
                        if (vertexArgs.containsKey("initialInput") && vertexArgs.get("initialInput").equals("true")) {
                            isInitialInput = true;
                        }
                        if (isInitialInput) {
                            // System.out.println("Uninitialized set found, assuming it is on master node to create file paths...");
                            String pathInMaster = vertexArgs.get("path");
                            pathInMaster = pathInMaster.replace(Configuration.globalConfig.initialInputsGraphMLAlias, Hermes.hermes.initialInputsFolderPathOnMaster);
                            File[] files = (new File(pathInMaster)).listFiles();
                            for (int j = 0; j < files.length; j++) {
                                if (!files[j].exists()) {
                                    System.out.println("FATAL ERROR, input file does not exist on master");
                                    System.out.println(files[j].getAbsolutePath());
                                    System.exit(1);
                                }
                                String relativePath = vertexArgs.get("path") + files[j].getName();
                                relativePath = relativePath.replace(Configuration.globalConfig.initialInputsGraphMLAlias, Hermes.hermes.initialInputsFolderPathOnMaster);
                                //System.out.println("[][][][][ "+relativePath);
                                boolean isFinalOutputBool = false;
                                boolean isDir = false;
                                String fileType = getFileType(vertexArgs);
                                if (vertexArgs.containsKey("finalOutput")) {
                                    String isFinalOutput = vertexArgs.get("finalOutput");
                                    if (isFinalOutput.equals("true")) {
                                        isFinalOutputBool = true;
                                    }
                                }

                                if (vertexArgs.containsKey("isDir")) {
                                    String isDirString = vertexArgs.get("isDir");
                                    if (isDirString.equals("true")) {
                                        isDir = true;
                                    }
                                }

                                DataFile file = new DataFile(relativePath, fileType, isInitialInput, isFinalOutputBool, isDir);
                                File f = new File(file.pathInResource.get(DataFile.initResourceNode));

                                DataFileEvaluationFunctions.selector(f.getAbsolutePath(), file.metrics, Configuration.globalConfig.inputOutputFileAssessment, null);

//                                if (file.realFileSizeInB == 0.0) {//|| file.realFileSizeCustom == 0.0) {
//                                    System.out.println("FATAL ERROR, initial input size and custom size values not set");
//                                    System.exit(0);
//                                }
                                in.add(file);
                            }
                        } else {
                        }

                    }
                    verticestoData.put(currentVertex, in);
                }
                currentComponentInputsOrdered.put(Integer.valueOf(inputID), verticestoData.get(currentVertex));
            }
        }
        return currentComponentInputsOrdered;
    }

    public TreeMap<Integer, ArrayList<DataFile>> addOutputsToVertexToData(ArrayList<Vertex> outputVertexes, Vertex componentVertex, HashMap<String, String> vertexArgs, Integer numOfRuns, String expression) throws IOException {
        TreeMap<Integer, ArrayList<DataFile>> currentComponentOutputsOrdered = new TreeMap<Integer, ArrayList<DataFile>>();

        for (int i = 0; i < outputVertexes.size(); i++) {
            Vertex currentOutputVertex = outputVertexes.get(i);
            String outputID = "invalid";
            Iterable<Edge> currentVertexEdges = currentOutputVertex.getEdges(Direction.IN, "_default");
            Iterator<Edge> edgesIterator = currentVertexEdges.iterator();
            while (edgesIterator.hasNext()) {
                Edge edge = edgesIterator.next();
                Vertex currentParent = edge.getVertex(Direction.OUT);
                if (currentParent == componentVertex) {

                    HashMap<String, String> outputEdgeArgs = parseVertexArgs((String) edge.getProperty("description"));
                    outputID = outputEdgeArgs.get("outputID");
                    //System.out.println("!!!!outputID = " + outputID);
                }
            }
            // System.out.println("OUTPUT ID IS: " + outputID);

            String currentVertexDescription = (String) currentOutputVertex.getProperty("description");
            HashMap<String, String> vertexArgsOfOut = parseVertexArgs(currentVertexDescription);

            ArrayList<DataFile> out = new ArrayList<DataFile>();

            if (vertexArgsOfOut.get("type").equals("DataFile")) {
                String fileType = getFileType(vertexArgsOfOut);
                String nameI = vertexArgsOfOut.get("path");
                String fileName = "/CompName_" + vertexArgs.get("name") + "_" + nameI;//+"." + fileType;

                boolean isInitialInput = false;
                boolean isFinalOutputBool = false;
                boolean isDir = false;

                if (vertexArgsOfOut.containsKey("finalOutput")) {
                    String isFinalOutput = vertexArgsOfOut.get("finalOutput");
                    if (isFinalOutput.equals("true")) {
                        isFinalOutputBool = true;
                    }
                }
                if (vertexArgsOfOut.containsKey("isDir")) {
                    String isDirString = vertexArgsOfOut.get("isDir");
                    if (isDirString.equals("true")) {
                        isDir = true;
                    }
                }

                DataFile file = new DataFile(fileName, fileType, isInitialInput, isFinalOutputBool, isDir);
                //System.out.println(fileName+" "+isDir);
                out.add(file);

            } else if (vertexArgsOfOut.get("type").equals("DataFile[]")) {
                int numOfOutputs = numOfRuns;
                if (expression.contains("span")) {
                    //System.out.println(vertexArgs.get("spansize"));
                    numOfOutputs = Integer.valueOf(vertexArgs.get("spansize"));
                    //System.out.println("SPAN OUTS: " + numOfOutputs);
                }
                for (int j = 0; j < numOfOutputs; j++) {
                    String nameI = vertexArgsOfOut.get("path");
                    //System.out.println("PATH IS :" + nameI);
                    String fileType = getFileType(vertexArgsOfOut);
                    String fileName;
                    fileName = "/CompName_" + vertexArgs.get("name") + "_" + nameI + String.valueOf(j);
                    if (expression.contains("span")) {
                        numOfOutputs = Integer.valueOf(vertexArgs.get("spansize"));
                        fileName = vertexArgs.get("spanprefix") + String.valueOf(j);
                    }

                    boolean isInitialInput = false;

                    boolean isFinalOutputBool = false;
                    boolean isDir = false;

                    if (vertexArgsOfOut.containsKey("finalOutput")) {
                        String isFinalOutput = vertexArgsOfOut.get("finalOutput");
                        if (isFinalOutput.equals("true")) {
                            isFinalOutputBool = true;
                        }
                    }
                    if (vertexArgsOfOut.containsKey("isDir")) {
                        String isDirString = vertexArgsOfOut.get("isDir");
                        if (isDirString.equals("true")) {
                            isDir = true;
                        }
                    }

                    //System.out.println("output datafile[] :" + fileName);
                    DataFile file = new DataFile(fileName, fileType, isInitialInput, isFinalOutputBool, isDir);
                    out.add(file);
                }
            } else {
                // System.out.println("--------------_FATAL ERROR, neither Datafile nor DataFile[]");
            }

            verticestoData.put(currentOutputVertex, out);
            currentComponentOutputsOrdered.put(Integer.valueOf(outputID), verticestoData.get(currentOutputVertex));
        }
        return currentComponentOutputsOrdered;
    }

    public ArrayList<ArrayList<DataFile>> buildInputsForEachComponentRunFromExpression(String expression, TreeMap<Integer, ArrayList<DataFile>> currentComponentInputsOrdered) {
        ArrayList<ArrayList<DataFile>> inputsForEachRun = new ArrayList<ArrayList<DataFile>>();
        /* start: check if >2 arrays */
        int numOfArrayInputs = 0;
        //System.out.println("size of ins: " + currentComponentInputsOrdered.size());
        for (Map.Entry<Integer, ArrayList<DataFile>> entry : currentComponentInputsOrdered.entrySet()) {
            //System.out.println(entry.getValue().get(0).fileName);
            if (entry.getValue().size() > 1) {
                numOfArrayInputs++;
            }
            if (numOfArrayInputs > 2) {
                System.out.println("Fatal error, more than 2 arrays as inputs are not supported yet.. exiting");
                System.exit(1);
            }
        }

        if (expression.contains("reduction")) {
            inputsForEachRun.add(currentComponentInputsOrdered.get(1));
            return inputsForEachRun;
        } else if (expression.contains("span")) {

        }

        //System.out.println("number of array inputs: " + numOfArrayInputs);
        /* end: check if >2 arrays */
        if (numOfArrayInputs == 0) { //case serial
            inputsForEachRun = buildSerialInputs(currentComponentInputsOrdered);

        } else if (numOfArrayInputs == 1) {//for all of array, all other serial
            inputsForEachRun = buildOneArrayOtherSerialInputs(currentComponentInputsOrdered);

        } else if (numOfArrayInputs == 2) {//2 arrays detected, need to check expression for + or *

            if (expression.contains("+")) {

                inputsForEachRun = buildTwoArraysAdditionOtherSerialInputs(currentComponentInputsOrdered);

            } else if (expression.contains("*")) {
                inputsForEachRun = buildTwoArraysCartesianOtherSerialInputs(currentComponentInputsOrdered);

            } else {
                System.out.println("Fatal error, unsupported expression detected, not containing + or * exiting.. expression was : " + expression);
                System.exit(1);
            }
        }

        return inputsForEachRun;
    }

    public ArrayList<ArrayList<DataFile>> buildOutputsForEachComponentRunFromExpressionAndRunSize(String expression, TreeMap<Integer, ArrayList<DataFile>> currentComponentOutputsOrdered, Integer numOfRuns) {
        ArrayList<ArrayList<DataFile>> outputsForEachRun = new ArrayList<ArrayList<DataFile>>();

        if (expression.contains("reduction")) {
            outputsForEachRun = buildOutputsReductionOrReduction(currentComponentOutputsOrdered);
        } else if (expression.contains("span")) {
            outputsForEachRun = buildOutputsReductionOrReduction(currentComponentOutputsOrdered);
        } else if (numOfRuns == 1) {
            outputsForEachRun = serialOutputs(currentComponentOutputsOrdered);
        } else {
            outputsForEachRun = buildOutputsStandard(currentComponentOutputsOrdered, numOfRuns);
        }

        return outputsForEachRun;
    }

    public void buildComponentsFromVertex(Vertex componentVertex) throws ParserConfigurationException, SAXException, IOException {
        HashMap<String, String> vertexArgs = parseVertexArgs((String) componentVertex.getProperty("description"));
        String name = vertexArgs.get("name");
        System.out.println("Component name: " + name);

        ArrayList<Vertex> inputVertexes = findVertexParents(componentVertex);
        TreeMap<Integer, ArrayList<DataFile>> currentComponentInputsOrdered = addInputsToVertexToData(inputVertexes, componentVertex);

        String expression = getExpType(vertexArgs);//.get("exp");
        String threadsMin = getThreadsMin(vertexArgs);//"1";
//        if (vertexArgs.containsKey("threadsMin")) {
//            threadsMin = vertexArgs.get("threadsMin");
//        }
        String threadsMax = getThreadsMax(vertexArgs);;
//        if (vertexArgs.containsKey("threadsMax")) {
//            threadsMax = vertexArgs.get("threadsMax");
//        }

        ArrayList<ArrayList<DataFile>> inputsForEachRun = buildInputsForEachComponentRunFromExpression(expression, currentComponentInputsOrdered);

        int numOfRuns = inputsForEachRun.size();
        //System.out.println("num of runs: " + numOfRuns);

        ArrayList<Vertex> outputVertexes = findVertexChildren(componentVertex);
        TreeMap<Integer, ArrayList<DataFile>> currentComponentOutputsOrdered = addOutputsToVertexToData(outputVertexes, componentVertex, vertexArgs, numOfRuns, expression);

        ArrayList<ArrayList<DataFile>> outputsForEachRun = buildOutputsForEachComponentRunFromExpressionAndRunSize(expression, currentComponentOutputsOrdered, numOfRuns);

        String command = vertexArgs.get("command");
        System.out.println("numOfRuns: " + numOfRuns);
        //System.out.println(inputsForEachRun.size());
        //System.out.println(outputsForEachRun.size());
        for (int i = 0; i < numOfRuns; i++) {
            ArrayList<DataFile> inputs = inputsForEachRun.get(i);
            ArrayList<DataFile> outputs = outputsForEachRun.get(i);//new ArrayList<DataFile>();
            Component currentComponent = new Component(name, command, inputs, outputs, ExecutionSite.defaultResourceNode, expression, threadsMin, threadsMax);
            ArrayList<TreeNode> dependencies = new ArrayList<TreeNode>();

            for (int j = 0; j < outputs.size(); j++) {
                outputs.get(j).createdByComponent = currentComponent;
            }

            boolean allInitial = true;
            for (int j = 0; j < inputs.size(); j++) {
                if (inputs.get(j).iSinitialInput == false) {
                    allInitial = false;
                }
            }

            if (allInitial) {
                //System.out.println("all are initial");
                if (!dependencies.contains(root)) {
                    dependencies.add(root);
                }

            } else {
                for (int j = 0; j < inputs.size(); j++) {
                    if (inputs.get(j).iSinitialInput) {
                        if (!dependencies.contains(root)) {
                            if (!dependencies.contains(root)) {
                                dependencies.add(root);
                            }
                        }
                    } else if (!dependencies.contains(inputs.get(j).createdByComponent.ofNode)) {
                        dependencies.add(inputs.get(j).createdByComponent.ofNode);
                    }

                    //System.out.println("Dependent on node ID: " + inputs.get(j).createdByComponent.ofNode.id);
                }
            }
            TreeNode newNode = new TreeNode(currentComponent, dependencies);
            keepTreeNodes.add(newNode);
        }

    }

    public ArrayList<ArrayList<DataFile>> buildOutputsReductionOrReduction(TreeMap<Integer, ArrayList<DataFile>> currentComponentOutputsOrdered) {
        ArrayList<ArrayList<DataFile>> ret = new ArrayList<ArrayList<DataFile>>();
        ArrayList<DataFile> tmp = new ArrayList<DataFile>();
        ret.add(currentComponentOutputsOrdered.get(1));
        return ret;
    }

    public ArrayList<ArrayList<DataFile>> buildOutputsStandard(TreeMap<Integer, ArrayList<DataFile>> currentComponentOutputsOrdered, Integer numOfRuns) {
        ArrayList<ArrayList<DataFile>> ret = new ArrayList<ArrayList<DataFile>>();

        for (int i = 0; i < numOfRuns; i++) {
            ArrayList<DataFile> tmp = new ArrayList<DataFile>();
            for (Map.Entry<Integer, ArrayList<DataFile>> entry : currentComponentOutputsOrdered.entrySet()) {
                if (entry.getValue().size() > 1) {
                    tmp.add(entry.getValue().get(i));
                } else {
                    tmp.add(entry.getValue().get(0));
                }
            }
            ret.add(tmp);
        }

        return ret;
    }

    public ArrayList<ArrayList<DataFile>> buildTwoArraysAdditionOtherSerialInputs(TreeMap<Integer, ArrayList<DataFile>> currentComponentInputsOrdered) {
        ArrayList<ArrayList<DataFile>> ret = new ArrayList<ArrayList<DataFile>>();
        ArrayList<DataFile> array1 = new ArrayList<DataFile>();
        ArrayList<DataFile> array2 = new ArrayList<DataFile>();
        boolean array1Found = false;
        for (Map.Entry<Integer, ArrayList<DataFile>> entry : currentComponentInputsOrdered.entrySet()) {
            if (entry.getValue().size() > 1) {
                array1 = entry.getValue();
                array1Found = true;
            }
            if (entry.getValue().size() > 1 && array1Found) {
                array2 = entry.getValue();
                break;
            }
        }

        if (array1.size() != array2.size()) {
            System.out.println("Fatal error, twoArraysAddition operation are not equal in size.. exiting");
            System.exit(1);
        }

        for (int i = 0; i < array1.size(); i++) {
            ArrayList<DataFile> tmp = new ArrayList<DataFile>();

            for (Map.Entry<Integer, ArrayList<DataFile>> entry : currentComponentInputsOrdered.entrySet()) {
                if (entry.getValue().size() == 1) {
                    tmp.add(entry.getValue().get(0));
                } else {
                    tmp.add(entry.getValue().get(i));
                }
            }
            ret.add(tmp);
        }
        return ret;
    }

    public ArrayList<ArrayList<DataFile>> buildTwoArraysCartesianOtherSerialInputs(TreeMap<Integer, ArrayList<DataFile>> currentComponentInputsOrdered) {
        ArrayList<ArrayList<DataFile>> ret = new ArrayList<ArrayList<DataFile>>();
        ArrayList<DataFile> array1 = new ArrayList<DataFile>();
        ArrayList<DataFile> array2 = new ArrayList<DataFile>();
        boolean array1Found = false;
        for (Map.Entry<Integer, ArrayList<DataFile>> entry : currentComponentInputsOrdered.entrySet()) {
            if (entry.getValue().size() > 1) {
                array1 = entry.getValue();
                array1Found = true;
            }
            if (entry.getValue().size() > 1 && array1Found) {
                array2 = entry.getValue();
                break;
            }
        }

        for (int i = 0; i < array1.size(); i++) {
            for (int j = 0; j < array2.size(); j++) {
                ArrayList<DataFile> tmp = new ArrayList<DataFile>();

                for (Map.Entry<Integer, ArrayList<DataFile>> entry : currentComponentInputsOrdered.entrySet()) {
                    boolean first = true;
                    if (entry.getValue().size() == 1) {
                        tmp.add(entry.getValue().get(0));
                    } else if (first) {
                        tmp.add(entry.getValue().get(i));
                        first = false;
                    } else {
                        tmp.add(entry.getValue().get(j));
                    }
                }
                ret.add(tmp);
            }
        }
        return ret;
    }

    public ArrayList<ArrayList<DataFile>> buildOneArrayOtherSerialInputs(TreeMap<Integer, ArrayList<DataFile>> currentComponentInputsOrdered) {
        ArrayList<ArrayList<DataFile>> ret = new ArrayList<ArrayList<DataFile>>();
        ArrayList<DataFile> array = new ArrayList<DataFile>();
        for (Map.Entry<Integer, ArrayList<DataFile>> entry : currentComponentInputsOrdered.entrySet()) {
            if (entry.getValue().size() > 1) {
                array = entry.getValue();
                break;
            }
        }

        for (int i = 0; i < array.size(); i++) {
            ArrayList<DataFile> tmp = new ArrayList<DataFile>();
            for (Map.Entry<Integer, ArrayList<DataFile>> entry : currentComponentInputsOrdered.entrySet()) {
                if (entry.getValue().size() == 1) {
                    tmp.add(entry.getValue().get(0));
                } else {
                    tmp.add(array.get(i));
                }
            }
            ret.add(tmp);
        }
        return ret;
    }

    public ArrayList<ArrayList<DataFile>> buildSerialInputs(TreeMap<Integer, ArrayList<DataFile>> currentComponentInputsOrdered) {
        ArrayList<ArrayList<DataFile>> ret = new ArrayList<ArrayList<DataFile>>();
        ArrayList<DataFile> tmp = new ArrayList<DataFile>();

        for (int i = 0; i < currentComponentInputsOrdered.size(); i++) {
            Integer key = i + 1;//String.valueOf(i + 1);
            tmp.add(currentComponentInputsOrdered.get(key).get(0));
        }

        ret.add(tmp);
        return ret;
    }

    public ArrayList<ArrayList<DataFile>> serialOutputs(TreeMap<Integer, ArrayList<DataFile>> currentComponentOutputsOrdered) {
        ArrayList<ArrayList<DataFile>> ret = new ArrayList<ArrayList<DataFile>>();
        ArrayList<DataFile> tmp = new ArrayList<DataFile>();

        for (int i = 0; i < currentComponentOutputsOrdered.size(); i++) {
            Integer key = i + 1;
            tmp.add(currentComponentOutputsOrdered.get(key).get(0));
        }
        ret.add(tmp);
        return ret;
    }

    public ArrayList<ArrayList<DataFile>> BuildNto1Inputs(ArrayList<DataFile> A, ArrayList<DataFile> B) {
        ArrayList<ArrayList<DataFile>> ret = new ArrayList<ArrayList<DataFile>>();
        if (A.size() > B.size()) {
            for (int i = 0; i < A.size(); i++) {
                ArrayList<DataFile> tmp = new ArrayList<DataFile>();
                tmp.add(A.get(i));
                tmp.add(B.get(0));
                ret.add(tmp);
                //System.out.println(Arrays.toString(tmp.toArray()));
            }
        } else {
            for (int i = 0; i < B.size(); i++) {
                ArrayList<DataFile> tmp = new ArrayList<DataFile>();
                tmp.add(B.get(i));
                tmp.add(A.get(0));
                ret.add(tmp);
                //System.out.println(Arrays.toString(tmp.toArray()));
            }
        }
        return ret;
    }

    public HashMap<String, String> parseVertexArgs(String args) {
        if (args.equals("")) {
            return null;
        }
        HashMap<String, String> argsMap = new HashMap<String, String>();
        if (!args.contains(";")) {
            String[] in = args.split("=");
            argsMap.put(in[0], in[1]);
            return argsMap;
        }
        String[] tmp = args.split(";");
        for (int i = 0; i < tmp.length; i++) {
            String[] in = tmp[i].split("=");
            argsMap.put(in[0], in[1]);
        }
        return argsMap;
    }
}
