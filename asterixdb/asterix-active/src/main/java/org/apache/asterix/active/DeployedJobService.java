/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.active;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.transaction.management.service.transaction.JobIdFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;

/**
 * Provides functionality for running DeployedJobSpecs
 */
public class DeployedJobService {

    private static final Logger LOGGER = Logger.getLogger(DeployedJobService.class.getName());

    //To enable new Asterix JobId for separate job invocations
    private static final byte[] JOB_ID_PARAMETER_NAME = "jobIdParameter".getBytes();

    //pool size one (only running one thread at a time)
    private static final int poolSize = 1;

    //Starts running a deployed job specification periodically with an interval of "duration" seconds
    public static ScheduledExecutorService startRepetitiveDeployedJobSpec(DeployedJobSpecId distributedId,
            IHyracksClientConnection hcc, long duration, Map<byte[], byte[]> jobParameters, EntityId entityId) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(poolSize);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!runRepetitiveDeployedJobSpec(distributedId, hcc, jobParameters, duration, entityId)) {
                        scheduledExecutorService.shutdown();
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Job Failed to run for " + entityId.getExtensionName() + " "
                            + entityId.getDataverse() + "." + entityId.getEntityName() + ".", e);
                }
            }
        }, duration, duration, TimeUnit.MILLISECONDS);
        return scheduledExecutorService;
    }

    public static boolean runRepetitiveDeployedJobSpec(DeployedJobSpecId distributedId, IHyracksClientConnection hcc,
            Map<byte[], byte[]> jobParameters, long duration, EntityId entityId) throws Exception {
        long executionMilliseconds = runDeployedJobSpec(distributedId, hcc, jobParameters, entityId);
        if (executionMilliseconds > duration && LOGGER.isLoggable(Level.SEVERE)) {
            LOGGER.log(Level.SEVERE,
                    "Periodic job for " + entityId.getExtensionName() + " " + entityId.getDataverse() + "."
                            + entityId.getEntityName() + " was unable to meet the required period of " + duration
                            + " milliseconds. Actually took " + executionMilliseconds + " execution will shutdown"
                            + new Date());
            return false;
        }
        return true;
    }

    public synchronized static long runDeployedJobSpec(DeployedJobSpecId distributedId, IHyracksClientConnection hcc,
            Map<byte[], byte[]> jobParameters, EntityId entityId) throws Exception {
        JobId jobId;
        long startTime = Instant.now().toEpochMilli();

        //Add the Asterix Transaction Id to the map
        byte[] asterixJobId = String.valueOf(JobIdFactory.generateJobId().getId()).getBytes();
        byte[] jobIdParameter = JOB_ID_PARAMETER_NAME;
        jobParameters.put(jobIdParameter, asterixJobId);
        jobId = hcc.startJob(distributedId, jobParameters);

        hcc.waitForCompletion(jobId);
        long executionMilliseconds = Instant.now().toEpochMilli() - startTime;

        LOGGER.log(Level.INFO,
                "Deployed Job execution completed for " + entityId.getExtensionName() + " " + entityId.getDataverse()
                        + "."
                        + entityId.getEntityName() + ". Took " + executionMilliseconds + " milliseconds ");

        return executionMilliseconds;

    }

    @Override
    public String toString() {
        return "DeployedJobSpecService";
    }

}
