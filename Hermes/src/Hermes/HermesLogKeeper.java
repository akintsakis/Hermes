/*
 * Copyright 2016 thano.
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author thano
 */
public class HermesLogKeeper {

    static private BufferedWriter receiverLog;
    static private BufferedWriter senderLog;
    static private BufferedWriter fileRetrieverLog;

    static public void initializeLogs(String locationOfHermesRootFolder, String currentDate) {
        try {
            File logDir = new File(locationOfHermesRootFolder + "/WorkflowRunLogs/" + currentDate + "/");
            logDir.mkdirs();
            File receiverLogFile = new File(locationOfHermesRootFolder + "/WorkflowRunLogs/" + currentDate + "/receiverLog");
            File senderLogFile = new File(locationOfHermesRootFolder + "/WorkflowRunLogs/" + currentDate + "/senderLog");
            File fileRetrieverFile = new File(locationOfHermesRootFolder + "/WorkflowRunLogs/" + currentDate + "/fileRetrieverLog");
            receiverLog = new BufferedWriter(new FileWriter(receiverLogFile));
            senderLog = new BufferedWriter(new FileWriter(senderLogFile));
            fileRetrieverLog = new BufferedWriter(new FileWriter(fileRetrieverFile));
        } catch (IOException ex) {
            Logger.getLogger(HermesLogKeeper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static public void closeLogFiles() {
        try {
            receiverLog.close();
            senderLog.close();
            fileRetrieverLog.close();
        } catch (IOException ex) {
            Logger.getLogger(HermesLogKeeper.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    static public void logReceiver(String line) {
        logLine(receiverLog, line);
    }

    static public void logSender(String line) {
        logLine(senderLog, line);
    }

    static public void logFileRetriever(String line) {
        logLine(fileRetrieverLog, line);
    }

    static public void logLine(BufferedWriter logger, String line) {
        try {
            logger.write(line);
            logger.newLine();
            logger.flush();
        } catch (IOException ex) {
            Logger.getLogger(HermesLogKeeper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
