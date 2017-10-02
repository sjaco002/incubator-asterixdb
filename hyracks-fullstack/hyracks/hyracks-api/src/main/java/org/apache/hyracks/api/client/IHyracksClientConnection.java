/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.client;

import java.util.EnumSet;
import java.util.List;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;

/**
 * Interface used by clients to communicate with the Hyracks Cluster Controller.
 *
 * @author vinayakb
 */
public interface IHyracksClientConnection extends IClusterInfoCollector {
    /**
     * Gets the status of the specified Job.
     *
     * @param jobId
     *            JobId of the Job
     * @return {@link JobStatus}
     * @throws Exception
     */
    JobStatus getJobStatus(JobId jobId) throws Exception;

    /**
     * Gets detailed information about the specified Job.
     *
     * @param jobId
     *            JobId of the Job
     * @return {@link JobStatus}
     * @throws Exception
     */
    JobInfo getJobInfo(JobId jobId) throws Exception;

    /**
     * Cancel the job that has the given job id.
     *
     * @param jobId
     *            the JobId of the Job
     * @throws Exception
     */
    void cancelJob(JobId jobId) throws Exception;

    /**
     * Start the specified Job.
     *
     * @param jobSpec
     *            Job Specification
     * @throws Exception
     */
    JobId startJob(JobSpecification jobSpec) throws Exception;

    /**
     * Start the specified Job.
     *
     * @param jobSpec
     *            Job Specification
     * @param jobFlags
     *            Flags
     * @throws Exception
     */
    JobId startJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception;

    /**
     * Distribute the specified Job.
     *
     * @param jobSpec
     *            Job Specification
     * @param jobFlags
     *            Flags
     * @throws Exception
     */
    public long distributeJob(JobSpecification jobSpec) throws Exception;

    /**
     * Destroy the distributed graph for a pre-distributed job
     *
     * @param predestributedId
     *            The id of the predistributed job
     * @throws Exception
     */
    JobId destroyJob(JobId jobId) throws Exception;

    /**
     * Used to run a pre-distributed job by id (the same JobId will be returned)
     *
     * @param predestributedId
     *            The id of the predistributed job
     * @param jobParameters
     *            The serialized job parameters
     * @throws Exception
     */
    JobId startJob(JobId jobId) throws Exception;

    /**
     * Start the specified Job.
     *
     * @param acggf
     *            Activity Cluster Graph Generator Factory
     * @param jobFlags
     *            Flags
     * @throws Exception
     */
    JobId startJob(IActivityClusterGraphGeneratorFactory acggf, EnumSet<JobFlag> jobFlags) throws Exception;

    /**
     * Gets the IP Address and port for the DatasetDirectoryService wrapped in NetworkAddress
     *
     * @return {@link NetworkAddress}
     * @throws Exception
     */
    NetworkAddress getDatasetDirectoryServiceInfo() throws Exception;

    /**
     * Waits until the specified job has completed, either successfully or has
     * encountered a permanent failure.
     *
     * @param jobId
     *            JobId of the Job
     * @throws Exception
     */
    void waitForCompletion(JobId jobId) throws Exception;

    /**
     * Deploy the user-defined jars to the cluster
     *
     * @param jars
     *            a list of user-defined jars
     */
    DeploymentId deployBinary(List<String> jars) throws Exception;

    /**
     * undeploy a certain deployment
     *
     * @param deploymentId
     *            the id for the deployment to be undeployed
     */
    void unDeployBinary(DeploymentId deploymentId) throws Exception;

    /**
     * Start the specified Job.
     *
     * @param deploymentId
     *            the id of the specific deployment
     * @param jobSpec
     *            Job Specification
     * @throws Exception
     */
    JobId startJob(DeploymentId deploymentId, JobSpecification jobSpec) throws Exception;

    /**
     * Start the specified Job.
     *
     * @param deploymentId
     *            the id of the specific deployment
     * @param jobSpec
     *            Job Specification
     * @param jobFlags
     *            Flags
     * @throws Exception
     */
    JobId startJob(DeploymentId deploymentId, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception;

    /**
     * Start the specified Job.
     *
     * @param deploymentId
     *            the id of the specific deployment
     * @param acggf
     *            Activity Cluster Graph Generator Factory
     * @param jobFlags
     *            Flags
     * @throws Exception
     */
    JobId startJob(DeploymentId deploymentId, IActivityClusterGraphGeneratorFactory acggf, EnumSet<JobFlag> jobFlags)
            throws Exception;

    /**
     * Shuts down all NCs and then the CC.
     *
     * @param terminateNCService
     */
    void stopCluster(boolean terminateNCService) throws Exception;

    /**
     * Get details of specified node as JSON object
     *
     * @param nodeId
     *            id the subject node
     * @param includeStats
     * @param includeConfig
     * @return serialized JSON containing the node details
     * @throws Exception
     */
    String getNodeDetailsJSON(String nodeId, boolean includeStats, boolean includeConfig) throws Exception;

    /**
     * Gets thread dump from the specified node as a serialized JSON string
     */
    String getThreadDump(String node) throws Exception;

    /**
     * @return true if the connection is alive, false otherwise
     */
    boolean isConnected();

    /**
     * @return the hostname of the cluster controller
     */
    String getHost();

    /**
     * @return the port of the cluster controller
     */
    int getPort();
}
