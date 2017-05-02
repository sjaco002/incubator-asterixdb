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
package org.apache.hyracks.control.common.base;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.job.TaskAttemptDescriptor;

public interface INodeController {
    public void startTasks(DeploymentId deploymentId, JobId jobId, byte[] planBytes,
            List<TaskAttemptDescriptor> taskDescriptors, Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies,
            Set<JobFlag> flags, Map<String, byte[]> contextRuntTimeVarMap) throws Exception;

    public void abortTasks(JobId jobId, List<TaskAttemptId> tasks) throws Exception;

    public void cleanUpJoblet(JobId jobId, JobStatus status) throws Exception;

    public void reportPartitionAvailability(PartitionId pid, NetworkAddress networkAddress) throws Exception;

    public void deployBinary(DeploymentId deploymentId, List<URL> url) throws Exception;

    public void undeployBinary(DeploymentId deploymentId) throws Exception;

    public void distributeJob(JobId jobId, byte[] planBytes) throws Exception;

    public void destroyJob(JobId jobId) throws Exception;

    public void dumpState(String stateDumpId) throws Exception;

    public void shutdown(boolean terminateNCService) throws Exception;

    public void sendApplicationMessageToNC(byte[] data, DeploymentId deploymentId, String nodeId) throws Exception;

    public void takeThreadDump(String requestId) throws Exception;
}
