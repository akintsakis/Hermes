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
package client;

import java.util.ArrayList;

/**
 *
 * @author thanos
 */
public class JobRequest {

    public String command;
    public String NodeExecutionThreadId;
    public String componentID;
    public String NodeID;
    public String componentName;
    public boolean monitor = false;
    public Long timeout;
    public boolean inputOutputFileAssessment;
    public boolean terminate = false;
    public Integer slotsUsed;
    public String resourceName;

    public ArrayList<Integer> outputDataFileIds = new ArrayList<Integer>();
    public ArrayList<String> outputDataFilePaths = new ArrayList<String>();

    public ArrayList<Integer> inputDataFileIds = new ArrayList<Integer>();
    public ArrayList<String> inputDataFilePaths = new ArrayList<String>();

    public String inputsRealFileSizesInB;
    public String inputsRealFileSizesCustom;
    public boolean NodeExecutionThreadFinalized;

    public String threadsAssigned;
    public Integer threadsUsedByComponent;
    public Integer totalSystemCPUs;
    public String systemRAMSizeInMB;
    public String cpuSingleThreadedBenchmark;
    public String cpuMultiThreadedBenchmark;

}
