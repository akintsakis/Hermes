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
package client;

import static client.Client.talker;
import static client.Client.waitForCommand;
import com.google.gson.Gson;

import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;

public class Client {

    public static final String classPath = new File(Client.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getAbsolutePath();
    private static ServerSocket jobSocket;
    private static String masterAddress;
    private static String masterJobPort;

    public static long topInitTimeStamp;
    static Gson gson = new Gson();

    public static String waitForCommand() {
        try {
            Socket server = jobSocket.accept();
            String sender = server.getRemoteSocketAddress().toString();
            System.out.println(sender);
            DataInputStream in = new DataInputStream(server.getInputStream());
            String message = in.readUTF();
            server.close();
            return message;
        } catch (SocketTimeoutException s) {
            System.out.println("Socket timed out!");
            return "timeout exception";
            //break;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Error";

    }

    public static String executeBashScript(String[] cmd) {
        StringBuffer output1 = new StringBuffer();
        Process p;
        try {
            p = Runtime.getRuntime().exec(cmd);
            p.waitFor();
            BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line = "";
            while ((line = error.readLine()) != null) {
                output1.append(line + "\n");
            }
            p.destroy();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("exception " + e);
        }
        System.out.println("before");
        System.out.println(output1.toString());
        return output1.toString();
    }

    public static void terminateClient(BufferedWriter wr1, long startTime) throws IOException {
        System.out.println("END REACHED.. exiting");

        //JSONObject obj = new JSONObject();
        JobResponse jobResponse = new JobResponse();
        jobResponse.success = true;
        jobResponse.jobRequest.command = "Terminated";
        jobResponse.dateStarted = new Date();

        talker(gson.toJson(jobResponse), masterAddress, masterJobPort);
        wr1.write(String.valueOf((System.currentTimeMillis() - startTime)));
        wr1.newLine();
        wr1.write("success exit");
        wr1.newLine();
        wr1.write((new Date()).toString());
        wr1.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        int threadIdCount = 0;
        String baseDir = classPath.substring(0, classPath.indexOf("daemon"));
        File log = new File(baseDir + "/clientLogfile.log");
        File componentRuntimeLogsFolder = new File(baseDir + "/componentRuntimeLogs/");
        componentRuntimeLogsFolder.mkdirs();
        String componentRuntimeLogs = componentRuntimeLogsFolder.getAbsolutePath();
        FileWriter fwr1 = new FileWriter(log);
        BufferedWriter wr1 = new BufferedWriter(fwr1);
        wr1.write("init");
        wr1.newLine();
        wr1.flush();

        masterAddress = args[0];
        masterJobPort = args[1];
        jobSocket = new ServerSocket(Integer.parseInt(args[2]));
        Slave.jobSocket = jobSocket;
        Slave.masterAddress = masterAddress;
        Slave.masterJobPort = masterJobPort;
        System.out.println("CLIENT INITIATED LISTENING ON PORT " + args[2] + " SENDING ON " + masterAddress + ":" + masterJobPort);
        wr1.write("CLIENT INITIATED LISTENING ON PORT " + args[2] + " SENDING ON " + masterAddress + ":" + masterJobPort);
        wr1.newLine();
        wr1.flush();

        String topLog = baseDir + "/topLogFile";
        String diskLog = baseDir + "/pidStatLogFile";

        topInitTimeStamp = System.currentTimeMillis() / 1000L;
        String logCommand = "sh;-c;pidstat -dh 1 > " + diskLog;
        String[] logCommandArray = logCommand.split(";");
        Process logPidstat = Runtime.getRuntime().exec(logCommandArray);

        logCommand = "sh;-c;top -b -d 1 > " + topLog;
        logCommandArray = logCommand.split(";");
        Process topStat = Runtime.getRuntime().exec(logCommandArray);

        String compMonitoringFolder = classPath.substring(0, classPath.indexOf("daemon")) + "daemon/ComponentMonitoring/";

        String wrapper = compMonitoringFolder + "wrapper.sh";
        String monitor = compMonitoringFolder + "monitor.sh";

        String incoming;

        wr1.write("CLIENT INITIATED LISTENING ON PORT " + args[2] + " SENDING ON " + masterAddress + ":" + masterJobPort);
        wr1.newLine();
        wr1.flush();

        long startTime = System.currentTimeMillis();

        while (true) {
            if (topInitTimeStamp == 0) {
                List<String> lines = Files.readAllLines(new File(baseDir + "/topInitTimestamp").toPath(), StandardCharsets.UTF_8);
                topInitTimeStamp = Long.valueOf(lines.get(0));
            }

            wr1.flush();
            incoming = waitForCommand();
            System.out.println(incoming);
            JobRequest jobRequest = gson.fromJson(incoming, JobRequest.class);
            String[] tmp = incoming.split("_EOC_");

            if (terminateTrue(jobRequest)) {
                terminateClient(wr1, startTime);
                break;
            }
            Slave t = new Slave(threadIdCount++, jobRequest, wr1, wrapper, monitor, baseDir, componentRuntimeLogs, diskLog, topLog);
            t.start();

        }
    }

    public static Boolean terminateTrue(JobRequest jobRequest) {
        if (jobRequest.terminate) {
            return true;
        }
        return false;
    }

    public static void talker(String message, String serverName, String portString) {
        //String serverName = "Node1";
        int port = Integer.parseInt(portString);
        try {
            Socket client = new Socket(serverName, port);
            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out = new DataOutputStream(outToServer);
            out.writeUTF(message);
            out.close();
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
