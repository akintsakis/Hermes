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

/* This class represents a DataFile type of the Workflow */

package Hermes;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataFile {

    public static ExecutionSite initResourceNode = new ExecutionSite("localhost", "", "");
    public static ArrayList<DataFile> allDataFiles = new ArrayList<DataFile>();
    public static int dataFileIdCounter = 0;
    public static String currentFolder;
    int id;
    String fileName;
    String absolutePathResource;
    String type;

    boolean iSinitialInput = false;
    boolean isFinalOutput;
    boolean retrievedToMaster = false;
    boolean isDir = false;
    Component createdByComponent;

    double realFileSizeInB;
    double realFileSizeCustom;
    Map<ExecutionSite, String> pathInResource = new ConcurrentHashMap<ExecutionSite, String>();
    Map<ExecutionSite, Lock> resourceLocks = new ConcurrentHashMap<ExecutionSite, Lock>();
    String pathOnResourceAndComp;

    DataFile(String fileName, String type, boolean iSinitialInput, boolean isFinalOutput, boolean isDir) {
        this.id = dataFileIdCounter++;
        this.isFinalOutput = isFinalOutput;
        this.isDir = isDir;
        this.type = type;
        this.fileName = fileName + "." + this.type;

        this.iSinitialInput = iSinitialInput;
        
        if (iSinitialInput) {
            pathInResource.put(initResourceNode, fileName);
        }

        for (ExecutionSite resource : Hermes.hermes.resources) {
            Lock resourceLock = new Lock();
            resourceLocks.put(resource, resourceLock);
        }
        allDataFiles.add(this);
    }

    public void setFilePathForComponentAndResource(ExecutionSite r, Component c) {
        pathOnResourceAndComp = r.containerUserHomePath + currentFolder + "/" + c.folderId + "/" + fileName;
        pathInResource.put(r, pathOnResourceAndComp);
    }

}
