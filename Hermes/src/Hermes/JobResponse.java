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

import java.util.Date;

/**
 *
 * @author thanos
 */
public class JobResponse {

   public JobRequest jobRequest;
   public boolean success;
   public String error;
   public String mscError;
    //String date;
   public long runtime;
   public int wasRetriedTimes;
   public boolean timeOutKilled = false;
   public boolean outOfMemoryKilled = false;

   public String totalReads;
   public String totalWrites;
   public String topIntervalTimeInSeconds;
   public String averageCpuUsage;
   public String averageMemUsage;
   public String maxCpuUsage;
   public String maxMemoryUsage;

   public Date dateCompleted;
   public Date dateStarted;

    public String outputDataFileSizesBytes;
    public String outputDataFileSizesCustom;

}
