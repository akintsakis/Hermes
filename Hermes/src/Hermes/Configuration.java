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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Configuration {

    public static Configuration globalConfig = new Configuration();

    public final String sshKeyToAccessSites;
    public final String workflowTemporarySshKeyToAccessContainers;

    /* current run specific */
    public final String currentDate;
    //public final String workflowGraphMlInput;

    /* load from .config file or autodetect */
    public final String globalMasterIpAddress;
    public final String globalMasterListeningPort;
    public final String hermesWorkflowImageName;
    public final String hermesWorkflowContainerName;
    public final String masterUsername;

    public final Integer siteTunnelPortsStartFrom;
    public final Integer masterPortForwardsToSitesStartFrom;
    public final Integer fileRetrieverListeningPortOnMaster;
    public final Integer commsThreadListeningPortOnMaster;

    public final boolean inputOutputFileAssessment;
    public final boolean uploadAllIntermediateResults;
    public final boolean cloneGitCodeRepositoryOnEachRun;

    /* internal paths */
    public final String resourcesFolder;
    public final String configFile;
    public final String dockerbuildfile;
    //public final String initialInputsFolderPathOnMaster;
    public final String locationOfHermesRootFolder;
    public final String componentDescriptions;

    /* statically assigned parameters */
    public static final String timeOut = "7200";  //configured on client in seconds
    public final Integer maxPortRetries = 10000;
    public final int numberOfFileRetrievalRetries = 3;
    public static final String containerUsernameForSSH = "root";
    public static final String globalContainerUsername = "user";
    public final boolean useKeyNotPasswordForSSHtoSites = true;
    public final String initialInputsGraphMLAlias = "initialInputsFolder";
    public final String hermesWorkingDirName = "hermes_working_directory";
    public final String gitCodeRepo = "https://github.com/akintsakis/HermesComponents.git";
    
    public final String sampleInputFolder;
    public final String sampleWorkflow;

    /* inter program signals */
    public final String intraMasterTerminationSignal = "terminate_thread_immediately";

    private Properties getConfigFileProperties(String configFile) {
        FileInputStream generalConfigurationFile;
        try {
            generalConfigurationFile = new FileInputStream(configFile);
            Properties generalConfiguration = new Properties();
            generalConfiguration.load(generalConfigurationFile);
            generalConfigurationFile.close();
            return generalConfiguration;
        } catch (IOException ex) {
            Logger.getLogger(Configuration.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error loading configuration file, does configuration.config exist?");
            System.exit(1);
            return null;
        }
    }

    private String getMasterIp() {
        try {
            System.out.println("Autodetect IP requested...");
            URL whatismyip = new URL("http://checkip.amazonaws.com");
            BufferedReader in = new BufferedReader(new InputStreamReader(whatismyip.openStream()));
            String ip = in.readLine();
            System.out.println("Autodetected IP: " + ip);
            return ip;
        } catch (IOException ex) {
            Logger.getLogger(Configuration.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in auto getting of IP. Please assign it manually in config file");
            System.exit(1);
            return null;
        }
    }

    private Configuration() {
        currentDate = (new Date()).toString().replace(" ", "_").replace(":", "_");
        locationOfHermesRootFolder = (new File(Hermes.class.getProtectionDomain().getCodeSource().getLocation().getPath())).getParentFile().getParentFile().getParentFile().toString() + "/";
        System.out.println("Hermes root foler: " + locationOfHermesRootFolder);
        resourcesFolder = locationOfHermesRootFolder + "Sites/";
        configFile = locationOfHermesRootFolder + "configuration.config";
        dockerbuildfile = locationOfHermesRootFolder + "/InternalDockerFile/Dockerfile";
        workflowTemporarySshKeyToAccessContainers = locationOfHermesRootFolder + "/currentWorkflowGeneratedIdentityKey";
        componentDescriptions=locationOfHermesRootFolder + "Componentdescriptions";
        sampleWorkflow=locationOfHermesRootFolder + "/WorkflowGraphs/pangenome_analysis_workflow.graphml";
        sampleInputFolder = locationOfHermesRootFolder + "/WorkflowSampleInputs/SmallSampleInput/";
        

        Properties generalConfiguration = getConfigFileProperties(configFile);

        globalMasterListeningPort = generalConfiguration.getProperty("masterListeningPort");
        if (generalConfiguration.getProperty("masterIpAddress").equals("autodetect")) {            
            globalMasterIpAddress = getMasterIp();
        } else {
            globalMasterIpAddress = generalConfiguration.getProperty("masterIpAddress");
        }

        fileRetrieverListeningPortOnMaster = Integer.valueOf(generalConfiguration.getProperty("fileRetrieverListeningPortOnMaster"));
        commsThreadListeningPortOnMaster = Integer.valueOf(generalConfiguration.getProperty("commsThreadListeningPortOnMaster"));
        siteTunnelPortsStartFrom = Integer.valueOf(generalConfiguration.getProperty("siteTunnelPortsStartFrom"));
        masterPortForwardsToSitesStartFrom = Integer.valueOf(generalConfiguration.getProperty("masterPortForwardsToSitesStartFrom"));

        hermesWorkflowImageName = generalConfiguration.getProperty("hermesWorkflowImageName");
        hermesWorkflowContainerName = generalConfiguration.getProperty("hermesWorkflowContainerName");
        sshKeyToAccessSites = generalConfiguration.getProperty("pathToSSHKEy");

        if (!new File(sshKeyToAccessSites).exists()) {
            System.out.println("Path to SSH key for accessing sites is invalid. Key does not exist. Exiting...");
            System.exit(1);
            //throw (new IOException("Path to ssh key not valid.."));
        }

        if (generalConfiguration.containsKey("keepAllIntermediateInputs") && generalConfiguration.getProperty("keepAllIntermediateInputs").equals("true")) {
            uploadAllIntermediateResults = true;
        } else {
            uploadAllIntermediateResults = false;
        }

        if (generalConfiguration.containsKey("inputOutputFileAssessment") && generalConfiguration.getProperty("inputOutputFileAssessment").equals("true")) {
            inputOutputFileAssessment = true;
        } else {
            inputOutputFileAssessment = false;
        }

        if (generalConfiguration.containsKey("cloneGitCodeRepositoryOnEachRun") && generalConfiguration.getProperty("cloneGitCodeRepositoryOnEachRun").equals("true")) {
            cloneGitCodeRepositoryOnEachRun = true;
        } else {
            cloneGitCodeRepositoryOnEachRun = false;
        }

        masterUsername = System.getProperty("user.name");
        System.out.println(masterUsername);
    }

}
