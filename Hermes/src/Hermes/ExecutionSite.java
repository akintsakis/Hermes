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

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import net.neoremind.sshxcute.exception.TaskExecFailException;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.LocalPortForwarder;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExecutionSite extends Thread implements Comparable<ExecutionSite> {

    private static Lock portForwardsLock = new Lock();
    private Integer sitePortsStartFrom;

    public ArrayList<Component> componentsCurrentlyExecutingOnSites = new ArrayList<Component>();
    public static ExecutionSite defaultResourceNode;
    public final String name;
    public final int id;
    double ramSizeInMB;

    public int siteThreadCount;
    public int availableSlots;
    public Double cpuMultithreadedScore;
    public Double cpuSingleThreadedScore;

    final private String masterIpAddress;
    final private String ipAddress;
    //final private String password;
    String pathToKey;

    String containerUsername; //used for paths
    String containerUserHomePath;

    String sshHostUsername;
    String sshHostPort;

    String userHostHomePath;
    String tunnelIP = "127.0.0.1";
    String forwardedFileTransfersHostname = "127.0.0.1";
    boolean runsOnMaster = false;
    private Connection conn;

    //Integer masterTunnelPortSSH;
    Integer masterTunnelPortCommunications; //is the localport upon which the master sends signals that are forwarded to the remote site clients
    //used for port forwarding communication signals from master to sites    
    Integer tunnelPortStartForSSHconnecionsToSites;    //used for portforwarding ssh from sites to sites
    Integer tunnelPortSSHContainers; //used for local file retriever

    Integer sshTunnelForwardedContainersMasterListeningPort;// is the port that the containers reply back to the master

    Integer siteContainerSSHListeningPort = -1;
    Integer clientListeningPort = -1;
    static Integer masterPortForwardsToSitesStartFrom;

    public static HashMap<String, String> portMappings = new HashMap<String, String>();
    static HashMap<Integer, ExecutionSite> resourceNodesByForwardedPortForComms = new HashMap<Integer, ExecutionSite>();
    String dockerBuildFolder;

    //portForwards
    private LocalPortForwarder lpf2;
    private LocalPortForwarder lpf1;

    private final CyclicBarrier cyclicBarrier;
    private ArrayList<ExecutionSite> resources;

    ExecutionSite(String ipAddress, String password, String username) {
        sitePortsStartFrom = Configuration.globalConfig.siteTunnelPortsStartFrom;
        this.ipAddress = ipAddress;
        //this.password = password;
        this.sshHostUsername = username;
        if (username.equals("")) {
            userHostHomePath = System.getProperty("user.home") + "/";
        } else {
            userHostHomePath = "/home/" + username + "/";
        }
        cyclicBarrier = null;
        name = "host";
        id = -1;
        ramSizeInMB = 0.0;
        siteThreadCount = 0;
        masterIpAddress = "";
        conn = null;
        cpuMultithreadedScore = 0.0;
    }

    ExecutionSite(Integer id, String resourceConfigFile, ArrayList<ExecutionSite> resources, CyclicBarrier cyclicBarrier) throws IOException, TaskExecFailException, Exception {
        this.id = id;
        sitePortsStartFrom = Configuration.globalConfig.siteTunnelPortsStartFrom;
        this.cyclicBarrier = cyclicBarrier;
        this.resources = resources;

        FileInputStream generalConfigurationFile = new FileInputStream(resourceConfigFile);
        Properties siteConfiguration = new Properties();
        siteConfiguration.load(generalConfigurationFile);
        generalConfigurationFile.close();

        name = siteConfiguration.getProperty("name");
        if (name.equals("")) {
            System.out.println("Fatal error, name not provided in .site file... exiting...");
            System.exit(1);
        }
        containerUsername = Configuration.globalConfig.globalContainerUsername;
        containerUserHomePath = "/home/" + containerUsername + "/";
        //ipAddress = siteConfiguration.getProperty("ipAddress");
        if (siteConfiguration.getProperty("ipAddress").equals("")) {
            System.out.println("Fatal error, IP address not provided in .site file... exiting...");
            System.exit(1);
        }

        if (siteConfiguration.getProperty("ipAddress").equals(Configuration.globalConfig.globalMasterIpAddress) || siteConfiguration.getProperty("ipAddress").equals("localhost")) {
            masterIpAddress = "localhost";
            ipAddress = Configuration.globalConfig.globalMasterIpAddress;
            defaultResourceNode = this;
            runsOnMaster = true;
        } else {
            ipAddress = siteConfiguration.getProperty("ipAddress");
            masterIpAddress = Configuration.globalConfig.globalMasterIpAddress;
        }

        sshHostUsername = siteConfiguration.getProperty("username");
        if (sshHostUsername.equals("")) {
            System.out.println("Fatal error, username not provided in .site file... exiting...");
            System.exit(1);
        }
        userHostHomePath = "/home/" + sshHostUsername + "/";
        //password = siteConfiguration.getProperty("password");
        sshHostPort = siteConfiguration.getProperty("sshHostPort");
        if (sshHostPort.equals("")) {
            System.out.println("Fatal error, ssh port not provided in .site file... exiting...");
            System.exit(1);
        }
        pathToKey = Configuration.globalConfig.sshKeyToAccessSites;

        System.out.println("###Initializing Node " + this.id + " " + this.name);
        establishSSHConnection();
    }

    void establishSSHConnection() {
        try {
            if (lpf1 != null) {
                lpf1.close();
            }
            if (lpf2 != null) {
                lpf2.close();
            }
            if (conn != null) {
                conn.close();
            }

            if (runsOnMaster) {
                conn = new Connection("localhost", Integer.valueOf(sshHostPort));
            } else {
                conn = new Connection(ipAddress, Integer.valueOf(sshHostPort));
            }

            System.out.println("Connecting to :" + ipAddress + " port: " + sshHostPort);
            conn.connect();

            boolean isAuthenticated = false;
            //if (Configuration.globalConfig.useKeyNotPasswordForSSHtoSites) {
            File keyfile = new File(Configuration.globalConfig.sshKeyToAccessSites);
            String keyfilePass = "";
            isAuthenticated = conn.authenticateWithPublicKey(sshHostUsername, keyfile, keyfilePass);//conn.authenticateWithPassword(username, password);
            //} else {
            //     isAuthenticated = conn.authenticateWithPassword(sshHostUsername, password);
            //}

            if (isAuthenticated == false) {
                throw new IOException("Authentication failed on site: " + name + " check your username and key. If all else fails remove site");
            }

            //check if already initialized
            if (siteContainerSSHListeningPort < 0) {
                siteContainerSSHListeningPort = getNextFreePortOnSiteStartingFrom(conn);
            }

            //check if already initialized
            if (clientListeningPort < 0) {
                clientListeningPort = getNextFreePortOnSiteStartingFrom(conn);
            }

            if (!masterIpAddress.equals("localhost")) {
                setLPFTransfersSSH();
                setLPFComms();
            } else {
                masterTunnelPortCommunications = Integer.valueOf(clientListeningPort);
                tunnelPortSSHContainers = siteContainerSSHListeningPort;
                //System.out.println("!!!!!!!!!" + name + " masterTunnelComms " + masterTunnelPortCommunications);
                //System.out.println("!!!!!!!!!" + name + " tunnelPortSSHContainers " + tunnelPortSSHContainers);
            }

        } catch (IOException e) {
            System.out.println(e);
        }

    }

    public Integer getNextFreePortOnSiteStartingFrom(Connection conn) throws IOException {
        Integer startingFromPort = sitePortsStartFrom;
        //System.out.println(name+" starting port "+startingFromPort);
        String command = "ss -tln | awk 'NR > 1{gsub(/.*:/,\"\",$4); print $4}' | sort -un | awk -v n=" + String.valueOf(startingFromPort) + " '$0 < n {next}; $0 == n {n++; next}; {exit}; END {print n}'";
        command = "netstat  -atn | perl -0777 -ne '@ports = /tcp.*?\\:(\\d+)\\s+/imsg ; for $port (" + String.valueOf(startingFromPort) + "..65000) {if(!grep(/^$port$/, @ports)) { print $port; last } }'";
        try {
            sleep(300);
        } catch (InterruptedException ex) {
            Logger.getLogger(ExecutionSite.class.getName()).log(Level.SEVERE, null, ex);
        }
        Session sess = conn.openSession();
        sess.execCommand(command);
        InputStream stdout = new StreamGobbler(sess.getStdout());
        InputStream stderr = new StreamGobbler(sess.getStderr());
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(stderr));
        String line = stdoutReader.readLine();
        String line2 = stderrReader.readLine();
        sess.close();
        Integer freePort = Integer.valueOf(line);
        sitePortsStartFrom = freePort + 1;
        return freePort;
    }

    public void setSiteTunnels(ArrayList<ExecutionSite> resources) throws IOException {
        try {
            System.out.println("Setting up ssh tunnels on: " + name);
            String command;
            if (!runsOnMaster) {
                sshTunnelForwardedContainersMasterListeningPort = getNextFreePortOnSiteStartingFrom(conn);
                command = "docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " autossh -i /root/.ssh/identityToHosts -M 0 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -N -f -L " + String.valueOf(sshTunnelForwardedContainersMasterListeningPort) + ":localhost:" + String.valueOf(Configuration.globalConfig.commsThreadListeningPortOnMaster) + " " + Configuration.globalConfig.masterUsername + "@" + this.masterIpAddress;
                executeCommand(command, false, false);
            } else {
                sshTunnelForwardedContainersMasterListeningPort = Configuration.globalConfig.commsThreadListeningPortOnMaster;
            }
            sleep(500);
            for (int i = 0; i < resources.size(); i++) {
                if (resources.get(i).id != this.id) {
                    tunnelPortStartForSSHconnecionsToSites = getNextFreePortOnSiteStartingFrom(conn);
                    command = "docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " autossh -i /root/.ssh/identityToHosts -M 0 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -N -f -L " + String.valueOf(tunnelPortStartForSSHconnecionsToSites) + ":localhost:" + String.valueOf(resources.get(i).siteContainerSSHListeningPort) + " " + resources.get(i).sshHostUsername + "@" + resources.get(i).ipAddress;
                    executeCommand(command, true, true);
                    portMappings.put(String.valueOf(this.id) + "_" + String.valueOf(resources.get(i).id), String.valueOf(tunnelPortStartForSSHconnecionsToSites));
                    tunnelPortStartForSSHconnecionsToSites++;
                    sleep(500);
                }
            }
            System.out.println("Tunnels ready on " + name);
        } catch (InterruptedException ex) {
            Logger.getLogger(ExecutionSite.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createDockerKeys() throws JSchException, IOException {
        JSch jsch = new JSch();
        KeyPair kpair = KeyPair.genKeyPair(jsch, KeyPair.RSA);
        kpair.writePrivateKey(Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers);
        kpair.writePublicKey(Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers + ".pub", "");
        kpair.dispose();
    }

    public static void deleteDockerKey() {
        File f = new File(Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers);
        f.delete();
        f = new File(Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers + ".pub");
        f.delete();
    }

    public void run() {
        try {
            initializeResource();
            System.out.println(name + " initialized successfully");//, Num of resources initialized: " + (cyclicBarrier.getNumberWaiting() + 1) );
            sleep(500);
            setSiteTunnels(resources);
            cyclicBarrier.await();
            sleep(500);
            launchDaemonOnSite();

        } catch (Exception ex) {
            Logger.getLogger(ExecutionSite.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void initializeResource() throws TaskExecFailException, Exception {

        /* kill existing hermes container if exists */
        executeCommand("docker kill " + Configuration.globalConfig.hermesWorkflowContainerName, true, true);
        sleep(500);
        executeCommand("docker rm -f " + Configuration.globalConfig.hermesWorkflowContainerName, true, true);
        executeCommand("docker rmi " + Configuration.globalConfig.hermesWorkflowImageName, true, true);

        // WARNING REMOVES ALL CONTAINERS AND IMAGES
        // executeCommand("docker rm -f $(docker ps -a -q)", false, false);
        // executeCommand("docker rmi -f $(docker images -q)", false, false);

        /* launch new hermes workflow container */
        dockerBuildFolder = this.userHostHomePath + "/" + Configuration.globalConfig.hermesWorkingDirName + "/" + DataFile.currentFolder + "_myDockerBuild_toBeDeleted";
        executeCommand("rm -rf " + this.userHostHomePath + "/" + Configuration.globalConfig.hermesWorkingDirName + "/*", true, true);
        executeCommand("mkdir -p " + dockerBuildFolder, true, false);
        copyFile(Configuration.globalConfig.sshKeyToAccessSites, dockerBuildFolder);
        String keyToHostsName = (Paths.get(Configuration.globalConfig.sshKeyToAccessSites)).getFileName().toString();
        executeCommand("mv " + dockerBuildFolder + "/" + keyToHostsName + " " + dockerBuildFolder + "/identityToHosts", true, false);
        copyFile(Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers, dockerBuildFolder);
        copyFile(Configuration.globalConfig.workflowTemporarySshKeyToAccessContainers + ".pub", dockerBuildFolder);
        copyFile(Configuration.globalConfig.dockerbuildfile, dockerBuildFolder);
        copyDir(conn, Configuration.globalConfig.locationOfHermesRootFolder + "/daemon/dist", dockerBuildFolder + "/dist/", true);
        copyDir(conn, Configuration.globalConfig.locationOfHermesRootFolder + "/ComponentMonitoring", dockerBuildFolder + "/ComponentMonitoring/", true);
        executeCommand("cd " + dockerBuildFolder + " && sed -i 's/updatePortNumber/" + String.valueOf(siteContainerSSHListeningPort) + "/g' Dockerfile", true, true);
        System.out.println("Site: " + name + " Pulling docker image " + Configuration.dockerContainer + " ... if image does not exist locally it could take a while");
        executeCommand("docker pull " + Configuration.dockerContainer, true, false);
        System.out.println("Site: " + name + " Building workflow container...");
        executeCommand("cd " + dockerBuildFolder + " && docker build -t " + Configuration.globalConfig.hermesWorkflowImageName + " --rm=true .", true, false);
        executeCommand("rm " + dockerBuildFolder + "/identityToHosts", false, false);
        executeCommand("docker run --net=\"host\" -dti --name " + Configuration.globalConfig.hermesWorkflowContainerName + " " + Configuration.globalConfig.hermesWorkflowImageName, true, false);

        if (Configuration.globalConfig.cloneGitCodeRepositoryOnEachRun) {
            cloneGitCodeRepo();
        }

        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " find /home/user/Hermes/Components -type f -exec chmod +x {} +", true, false);
        //System.out.println("docker exec " + Configuration.hermesWorkflowContainerName + " cp /home/user/" + DataFile.currentFolder + "/daemon/ComponentMonitoring/plist /home/user/plist");
        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " mkdir /home/user/" + DataFile.currentFolder, true, false);
        executeCommand("docker cp " + dockerBuildFolder + "/dist/" + " " + Configuration.globalConfig.hermesWorkflowContainerName + ":/home/user/" + DataFile.currentFolder + "/daemon/", true, false);
        executeCommand("docker cp " + dockerBuildFolder + "/ComponentMonitoring/" + " " + Configuration.globalConfig.hermesWorkflowContainerName + ":/home/user/" + DataFile.currentFolder + "/daemon/ComponentMonitoring", true, false);
        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " cp /home/user/" + DataFile.currentFolder + "/daemon/ComponentMonitoring/plist /home/user/plist", true, false);
        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " screen -S dstatlogger -d -m dstat -T -t -c -l -m -d -n -p -r -i -g -s -y --float --output /home/user/" + DataFile.currentFolder + "/dstatlog", true, false);
        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " bash -c  \"echo $(date +%s) > /home/user/" + DataFile.currentFolder + "/topInitTimestamp\"", true, false);

        siteThreadCount = autoDetectThreadCountIfNotSet(conn);
        ramSizeInMB = autoDetectMaxMemoryIfNotSet(conn);
        cpuMultithreadedScore = autoDetectCpuMultithreadedPerformance(conn);
        cpuSingleThreadedScore = autoDetectCpuSinglethreadedPerformance(conn);

        availableSlots = siteThreadCount;
    }

    public void setLPFTransfersSSH() throws IOException {
        tunnelPortSSHContainers = masterPortForwardsToSitesStartFrom;//id+WorkflowOptimizer.globalConfig.masterPortForwardsToSitesStartFrom;//getNextFreePortOnSiteStartingFrom(conn);
        int maxRetries = Configuration.globalConfig.maxPortRetries;
        while (maxRetries > 0) {
            try {
                lpf2 = conn.createLocalPortForwarder(tunnelPortSSHContainers, "127.0.0.1", siteContainerSSHListeningPort);
                incrementMasterPortForwardsToSitesStartFrom();
                break;
            } catch (IOException ex) {
                maxRetries--;
                // System.out.println(ex.toString());
                System.out.println(name + " Port " + tunnelPortSSHContainers + " in use.... retrying +1");
                tunnelPortSSHContainers++;
                incrementMasterPortForwardsToSitesStartFrom();
            }
        }
        //System.out.println(name + " lpf2 established on: " + tunnelPortSSHContainers);
    }

    public void setLPFComms() throws IOException {
        masterTunnelPortCommunications = masterPortForwardsToSitesStartFrom;//id+ WorkflowOptimizer.globalConfig.masterPortForwardsToSitesStartFrom;//getNextFreePortOnSiteStartingFrom(conn);
        int maxRetries = Configuration.globalConfig.maxPortRetries;
        while (maxRetries > 0) {
            try {
                lpf1 = conn.createLocalPortForwarder(masterTunnelPortCommunications, "127.0.0.1", Integer.valueOf(clientListeningPort));
                incrementMasterPortForwardsToSitesStartFrom();
                break;
            } catch (IOException ex) {
                maxRetries--;
                System.out.println(name + " Port " + masterTunnelPortCommunications + " in use.... retrying +1");
                masterTunnelPortCommunications++;
                incrementMasterPortForwardsToSitesStartFrom();
            }
        }
        //System.out.println(name + " lpf1 established on: " + masterTunnelPortCommunications);
        resourceNodesByForwardedPortForComms.put(masterTunnelPortCommunications, this);
    }

    public void launchDaemonOnSite() throws IOException {
        String command;
        command = "docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " screen -S clientDaemon -d -m java -jar " + "/home/user/" + DataFile.currentFolder + "/daemon/Client.jar " + "127.0.0.1" + " " + sshTunnelForwardedContainersMasterListeningPort + " " + clientListeningPort + " > clientRedirectOut 2>&1";
        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " screen -S clientDaemon -d -m", false, false);
        try {
            sleep(300);
        } catch (InterruptedException ex) {
            Logger.getLogger(ExecutionSite.class.getName()).log(Level.SEVERE, null, ex);
        }
        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " screen -S clientDaemon -X stuff 'java -jar " + "/home/user/" + DataFile.currentFolder + "/daemon/Client.jar " + "127.0.0.1" + " " + sshTunnelForwardedContainersMasterListeningPort + " " + clientListeningPort + " -Xms256m -Xmx1024m > clientRedirectOut 2>&1'`echo -ne '\\015'`", false, false);
    }

    public void shutDownResource() throws TaskExecFailException, IOException {
        try {
            System.out.println("###Shutting Down Site " + this.id + " " + this.name);
            if (lpf2 != null) {
                lpf2.close();
            }
            if (lpf1 != null) {
                lpf1.close();
            }
            executeCommand("docker kill " + Configuration.globalConfig.hermesWorkflowContainerName, true, false);
            sleep(500);
            executeCommand("docker rm -f " + Configuration.globalConfig.hermesWorkflowContainerName, true, true);
            executeCommand("docker rmi " + Configuration.globalConfig.hermesWorkflowImageName, true, true);
            conn.close();
        } catch (InterruptedException ex) {
            Logger.getLogger(ExecutionSite.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public Integer autoDetectThreadCountIfNotSet(Connection conn) throws IOException {
        Integer siteThreads;
        Session sess = conn.openSession();
        sess.execCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " nproc");
        InputStream stdout = new StreamGobbler(sess.getStdout());
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
        String line = stdoutReader.readLine();
        siteThreads = Integer.valueOf(line);
        if (!(siteThreads > 0)) {
            System.out.println("Fatal error, could not autodetect threads on site: " + name + " ...please set the manually in .site file");
            System.exit(1);
        } else {
            System.out.println("Site :" + name + " autodetected threadcount: " + siteThreads);
        }
        sess.close();
        return siteThreads;
    }

    public Double autoDetectMaxMemoryIfNotSet(Connection conn) throws IOException {
        Double ramSize;
        Session sess = conn.openSession();
        sess.execCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " grep MemTotal /proc/meminfo | awk '{print $2}'");
        InputStream stdout = new StreamGobbler(sess.getStdout());
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
        String line = stdoutReader.readLine();
        ramSize = Double.valueOf(line);
        ramSize = ramSize / 1024.0;
        if (!(ramSize > 0.1)) {
            System.out.println("Fatal error, could not autodetect Ram in MB on site: " + name + " ...please set the manually in .site file");
            System.exit(1);
        } else {
            System.out.println("Site :" + name + " autodetected Ram in MB: " + ramSize);
        }
        sess.close();
        return ramSize;
    }

    private Double autoDetectCpuMultithreadedPerformance(Connection conn) throws IOException {
        Double cpuMultithreadedScore;
        Session sess = conn.openSession();
        sess.execCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " sysbench --test=cpu --num-threads=" + siteThreadCount + " --cpu-max-prime=30000 run | grep \"total time:\"");
        InputStream stdout = new StreamGobbler(sess.getStdout());
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
        String line = stdoutReader.readLine();
        line = line.replaceAll(" +", "");
        line = line.split(":")[1];
        line = line.substring(0, line.length() - 1);
        cpuMultithreadedScore = Double.valueOf(line);
        cpuMultithreadedScore = 1.0 / cpuMultithreadedScore;

        if (cpuMultithreadedScore == 0.1) {
            System.out.println("Fatal error, could not autodetect CPU power on site: " + name + " ...please set manually in .site file");
            System.exit(1);
        } else {
            System.out.println("Site :" + name + " CPU multithreaded score: " + cpuMultithreadedScore);
        }
        sess.close();
        return cpuMultithreadedScore;
        //return 0.1;
    }

    private Double autoDetectCpuSinglethreadedPerformance(Connection conn) throws IOException {
        Double cpuSinglethreadedScore;
        Session sess = conn.openSession();
        sess.execCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " sysbench --test=cpu --num-threads=1 --cpu-max-prime=20000 run | grep \"total time:\"");
        InputStream stdout = new StreamGobbler(sess.getStdout());
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
        String line = stdoutReader.readLine();
        line = line.replaceAll(" +", "");
        line = line.split(":")[1];
        line = line.substring(0, line.length() - 1);
        cpuSinglethreadedScore = Double.valueOf(line);
        cpuSinglethreadedScore = 1.0 / cpuSinglethreadedScore;

        if (cpuSinglethreadedScore == 0.1) {
            System.out.println("Fatal error, could not autodetect CPU power on site: " + name + " ...please set manually in .site file");
            System.exit(1);
        } else {
            System.out.println("Site :" + name + " CPU single threaded score: " + cpuSinglethreadedScore);
        }
        sess.close();
        return cpuSinglethreadedScore;
        //return 0.1;

    }

    public static void copyDir(Connection conn, String localDirectory, String remoteTargetDirectory, boolean firstTime) throws IOException {
        final String[] fileList = (new File(localDirectory).list());//curDir.list();
        if (firstTime) {
            Session sess = conn.openSession();
            sess.execCommand("mkdir " + remoteTargetDirectory);
            sess.waitForCondition(ChannelCondition.EOF, 0);
            sess.close();
        }

        for (String file : fileList) {
            final String fullFileName = localDirectory + "/" + file;
            //System.out.println(fullFileName);
            if (new File(fullFileName).isDirectory()) {
                //System.out.println("Directory");
                final String subDir = remoteTargetDirectory + "/" + file;
                Session sess = conn.openSession();
                sess.execCommand("mkdir " + subDir);
                sess.waitForCondition(ChannelCondition.EOF, 0);
                sess.close();
                copyDir(conn, fullFileName, subDir, false);
            } else {
                SCPClient scpc = conn.createSCPClient();
                scpc.put(fullFileName, remoteTargetDirectory);
            }
        }
    }

    public void copyFile(String local, String remote) throws IOException {
        SCPClient scpc = conn.createSCPClient();
        scpc.put(local, remote);
    }

    public void cloneGitCodeRepo() throws IOException {
        System.out.println("Site: " + name + " Cloning dependencies repo " + Configuration.globalConfig.gitCodeRepo);
        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " rm -rf /home/user/Hermes", true, false);
        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " git clone " + Configuration.globalConfig.gitCodeRepo, true, false);
        executeCommand("docker exec " + Configuration.globalConfig.hermesWorkflowContainerName + " mv ./HermesComponents ./Hermes", true, false);

    }

    public int executeCommand(String command, boolean suppressOutput, boolean supressErrorOutput) throws IOException {
        Session sess = conn.openSession();
        sess.execCommand(command);
        InputStream stdout = new StreamGobbler(sess.getStdout());
        InputStream stderr = new StreamGobbler(sess.getStderr());
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(stderr));

        if (!suppressOutput) {
            while (true) {
                String line = stdoutReader.readLine();
                if (line == null) {
                    break;
                }
                System.out.println(name + ": " + line);
            }
        }
        while (!supressErrorOutput) {
            String line = stderrReader.readLine();
            if (line == null) {
                break;
            }
            System.out.println(name + ": " + line);
        }
        int sessionStatus = 0;
        if (sess.getExitStatus() != null) {
            if (sess.getExitStatus() == 1) {
                System.out.println(name + ": " + "  ExitCodeFailure: " + sess.getExitStatus());
            }
            sessionStatus = sess.getExitStatus();
        }
        sess.close();
        return sessionStatus;
    }

    public void passCommandToClientWithinContainer(String command, Component component, Integer retries) {//String message, String serverName, Integer port, Integer retries) {
        if (component != null) {
            HermesLogKeeper.logSender("---SENDING--- --" + (new Date()).toString() + "-- @ " + name + "  Component: " + component.name + " command: " + command);
            //command = command + "_EOC_";
        }
        try {
            if (retries > 0) {
                Socket client = new Socket(tunnelIP, masterTunnelPortCommunications);
                OutputStream outToServer = client.getOutputStream();
                DataOutputStream out = new DataOutputStream(outToServer);
                out.writeUTF(command);
                out.close();
                client.close();
            } else {
                System.out.println("Faildure, max retries reached.. site " + name + " is down");
            }
        } catch (IOException e) {
            System.out.println("Connection exception connecting to container daemon on site.." + name);
            System.out.println("Error: " + e);
            System.out.println("Attempting to re-establish connection.., attempt:" + retries);

            try {
                sleep(3000);
            } catch (InterruptedException ex) {
                Logger.getLogger(ExecutionSite.class.getName()).log(Level.SEVERE, null, ex);
            }
            retries = retries - 1;
            establishSSHConnection();
            passCommandToClientWithinContainer(command, component, retries);
        }
    }

    public void revert() {
        availableSlots = siteThreadCount;
    }

    private static synchronized void incrementMasterPortForwardsToSitesStartFrom() {
        masterPortForwardsToSitesStartFrom++;
    }

    @Override
    public int compareTo(ExecutionSite o) {
        if (cpuMultithreadedScore > o.cpuMultithreadedScore) {
            return 1;
        } else if (cpuMultithreadedScore < o.cpuMultithreadedScore) {
            return -1;
        } else {
            return 0;
        }
    }

}
