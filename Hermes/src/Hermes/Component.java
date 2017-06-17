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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Component {

    TreeNode ofNode;
    public static int componentIdCounter = 0;
    public String name;
    public int id;
    
    public int extraRuns = 0;

    String runningWithNumOfThreads;
    String folderId;
    int threadsAssigned;
    int slots;
    NodeExecutionThread exec;
    String programPath;
    String path2;

    public static ArrayList<Component> allComponents = new ArrayList<Component>();

    ArrayList<DataFile> inputDataFiles = new ArrayList<DataFile>();
    ArrayList<DataFile> outputDataFiles = new ArrayList<DataFile>();

    String componentExecType;
    static final String userHomePath = System.getProperty("user.home");
    String originalCommand;
    String command;
    double cpuUsagePercentage;
    double maxRamUsageInMb;
    double maxHdSpaceInMb;
    double estimatedHdWritesInB;
    double estimatedComputationalComplexity;
    public ExecutionSite executedOnResource;

    /* cost evaluation metrics */
    int numberOfThreadsUsed;
    static double referenceCPUpower;
    ArrayList<Integer> threadMultiplier;
    double runtimeReference = 10;
    double runtimeReferenceInputSize = 10;

    double runtimeComplexity;
    double maxRAM;

    /* calculated quantities */
    double runtimeInMs;
    double dataTransferredAsInputFiles = 0.0;

    boolean executionCompleted = false;
    boolean lastExecutionFailed = false;

    String expression;
    public String threadsMin;
    public String threadsMax;

    public Component(String name, String command, ArrayList<DataFile> inputDataFiles, ArrayList<DataFile> outputDataFiles, ExecutionSite executedOnResource, String expression, String threadsMin, String threadsMax) throws ParserConfigurationException, SAXException, IOException {
        this.threadsMin = threadsMin;
        this.threadsMax = threadsMax;
        id = componentIdCounter++;
        this.expression = expression;
        this.command = command;
        this.originalCommand=command;
        this.inputDataFiles = inputDataFiles;
        this.outputDataFiles = outputDataFiles;
        this.executedOnResource = executedOnResource;
        loadParametersFromXml(Hermes.hermes.availableComponents.get(name));
        folderId = id + name;
        
//        if(name.equals("mclBlastProtein")) {
//            extraRuns = 18;
//        }
        
        allComponents.add(this);
    }
    
    public void restoreOriginalCommandForRerun() {
        command = originalCommand;
    }

    public void initComponentOnResourceNode() {
        if (programPath.equals("")) {
            //command = execCommand + " " + command;
        } else if (!path2.equals("")) {
            command = command.replace("programPath", executedOnResource.containerUserHomePath + programPath + " " + executedOnResource.containerUserHomePath + path2);
        } else {
            command = command.replace("programPath", executedOnResource.containerUserHomePath + programPath);
        }

        if (command.contains("threads")) {
            command = command.replace("threads", String.valueOf(threadsAssigned));
        }
        replaceInputsInCommand();
        replaceOutputsInCommand();
    }

    public void setAssignedThreads(int threadsAssigned) {
        this.threadsAssigned = threadsAssigned;
    }

    public double calculateFileTransfersRequiredForGivenSite(ExecutionSite site) {
        double dataToBeTransferred = 0.0;

        for (DataFile file : inputDataFiles) {
            if (file.realFileSizeInB == 0.0) {
                System.out.println("Warning: File has no size: " + file.id + file.fileName);
            }
            if (!file.pathInResource.containsKey(site)) {
                dataToBeTransferred = dataToBeTransferred + file.realFileSizeInB;
            }
        }
        return dataToBeTransferred;
    }

    public double calculateBurdenBasedOnInputSize() {
        double burden = 0.0;
        if (Configuration.globalConfig.inputOutputFileAssessment) {
            for (DataFile file : inputDataFiles) {
                burden = burden + file.realFileSizeCustom;
            }
        } else {
            for (DataFile file : inputDataFiles) {
                burden = burden + file.realFileSizeInB;
            }
        }
        return burden;
    }

    public void replaceInputsInCommand() {
        if (expression.contains("reduction")) {
            StringBuilder st = new StringBuilder();
            for (int i = 0; i < inputDataFiles.size(); i++) {
                st.append(inputDataFiles.get(i).pathInResource.get(executedOnResource)).append(" ");
            }
            command = command.replace("input[]", st.toString());
        } else {
            for (int i = 0; i < inputDataFiles.size(); i++) {
                String toReplace = "input" + String.valueOf(i + 1);
                String replaceWithPath = inputDataFiles.get(i).pathInResource.get(executedOnResource);
                if (replaceWithPath.contains(" ")) {
                    replaceWithPath = "\"" + replaceWithPath + "\"";
                }
                command = command.replace(toReplace, replaceWithPath);
            }
        }
    }

    public void replaceOutputsInCommand() {
        if (expression.contains("span")) {
            String path = outputDataFiles.get(0).pathInResource.get(executedOnResource);
            path = path.substring(0, path.lastIndexOf("/")) + "/";
            System.out.println(path);
            command = command.replace("output1", path);
        } else {
            for (int i = 0; i < outputDataFiles.size(); i++) {
                String toReplace = "output" + String.valueOf(i + 1);
                String replaceWithPath = outputDataFiles.get(i).pathInResource.get(executedOnResource);//executedOnResource.userHomePath + outputDataFiles.get(i).fileName;
                if (replaceWithPath.contains(" ")) {
                    replaceWithPath = "\"" + replaceWithPath + "\"";
                }
                command = command.replace(toReplace, replaceWithPath);
            }
        }
    }

    public void loadParametersFromXml(String file) throws ParserConfigurationException, SAXException, IOException {
        File fXmlFile = new File(file);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(fXmlFile);
        doc.getDocumentElement().normalize();
        NodeList nList = doc.getElementsByTagName("component");

        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                programPath = eElement.getElementsByTagName("path").item(0).getTextContent();
                path2 = eElement.getElementsByTagName("path2").item(0).getTextContent();
                name = eElement.getElementsByTagName("name").item(0).getTextContent();
                slots = Integer.valueOf(eElement.getElementsByTagName("slots").item(0).getTextContent());
                runtimeInMs = Double.valueOf(eElement.getElementsByTagName("defaultRunTimeInMs").item(0).getTextContent());
            }
        }
    }


    public void setComponentOutputPaths() {
        for (DataFile file : outputDataFiles) {
            file.createdByComponent = this;
            file.setFilePathForComponentAndResource(executedOnResource, this);
        }
    }
    
        public void clearComponentOutputPathsAfterFailedRun() {
        for (DataFile file : outputDataFiles) {
            file.createdByComponent = null;
            file.clearFilePathForComponentAndResource(executedOnResource);
        }
    }
    
    

    public void setOutputsRealFileSizesInB(String input) {
        String[] tmp = input.split(" ");
        for (int i = 0; i < outputDataFiles.size(); i++) {
            outputDataFiles.get(i).realFileSizeInB = Double.valueOf(tmp[i]);
        }
    }

    public void setInputsRealFileSizesInB(String input) {
        String[] tmp = input.split(" ");
        for (int i = 0; i < inputDataFiles.size(); i++) {
            inputDataFiles.get(i).realFileSizeInB = Double.valueOf(tmp[i]);
        }
    }

    public void setOutputsRealFileSizesCustom(String input) {
        String[] tmp = input.split(" ");
        for (int i = 0; i < outputDataFiles.size(); i++) {
            outputDataFiles.get(i).realFileSizeCustom = Double.valueOf(tmp[i]);
        }
    }

    public void setInputsRealFileSizesCustom(String input) {
        String[] tmp = input.split(" ");
        for (int i = 0; i < inputDataFiles.size(); i++) {
            inputDataFiles.get(i).realFileSizeCustom = Double.valueOf(tmp[i]);
        }
    }



}
