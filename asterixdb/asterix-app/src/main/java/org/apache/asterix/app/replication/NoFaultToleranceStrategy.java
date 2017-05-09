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
package org.apache.asterix.app.replication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.asterix.app.nc.task.BindMetadataNodeTask;
import org.apache.asterix.app.nc.task.CheckpointTask;
import org.apache.asterix.app.nc.task.ExternalLibrarySetupTask;
import org.apache.asterix.app.nc.task.LocalRecoveryTask;
import org.apache.asterix.app.nc.task.MetadataBootstrapTask;
import org.apache.asterix.app.nc.task.ReportMaxResourceIdTask;
import org.apache.asterix.app.nc.task.StartLifecycleComponentsTask;
import org.apache.asterix.app.replication.message.NCLifecycleTaskReportMessage;
import org.apache.asterix.app.replication.message.StartupTaskRequestMessage;
import org.apache.asterix.app.replication.message.StartupTaskResponseMessage;
import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.replication.IFaultToleranceStrategy;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class NoFaultToleranceStrategy implements IFaultToleranceStrategy {

    private static final Logger LOGGER = Logger.getLogger(NoFaultToleranceStrategy.class.getName());
    IClusterStateManager clusterManager;
    private String metadataNodeId;
    private Set<String> pendingStartupCompletionNodes = new HashSet<>();
    private ICCMessageBroker messageBroker;

    @Override
    public void notifyNodeJoin(String nodeId) throws HyracksDataException {
        pendingStartupCompletionNodes.add(nodeId);
    }

    @Override
    public void notifyNodeFailure(String nodeId) throws HyracksDataException {
        pendingStartupCompletionNodes.remove(nodeId);
        clusterManager.updateNodePartitions(nodeId, false);
        if (nodeId.equals(metadataNodeId)) {
            clusterManager.updateMetadataNode(metadataNodeId, false);
        }
        clusterManager.refreshState();
    }

    @Override
    public void process(INCLifecycleMessage message) throws HyracksDataException {
        switch (message.getType()) {
            case STARTUP_TASK_REQUEST:
                process((StartupTaskRequestMessage) message);
                break;
            case STARTUP_TASK_RESULT:
                process((NCLifecycleTaskReportMessage) message);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.UNSUPPORTED_MESSAGE_TYPE, message.getType().name());
        }
    }

    @Override
    public IFaultToleranceStrategy from(ICCServiceContext serviceCtx, IReplicationStrategy replicationStrategy) {
        NoFaultToleranceStrategy ft = new NoFaultToleranceStrategy();
        ft.messageBroker = (ICCMessageBroker) serviceCtx.getMessageBroker();
        return ft;
    }

    @Override
    public void bindTo(IClusterStateManager clusterManager) {
        this.clusterManager = clusterManager;
        metadataNodeId = clusterManager.getCurrentMetadataNodeId();
    }

    private void process(StartupTaskRequestMessage msg) throws HyracksDataException {
        final String nodeId = msg.getNodeId();
        List<INCLifecycleTask> tasks = buildNCStartupSequence(msg.getNodeId(), msg.getState());
        StartupTaskResponseMessage response = new StartupTaskResponseMessage(nodeId, tasks);
        try {
            messageBroker.sendApplicationMessageToNC(response, msg.getNodeId());
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private void process(NCLifecycleTaskReportMessage msg) throws HyracksDataException {
        pendingStartupCompletionNodes.remove(msg.getNodeId());
        if (msg.isSuccess()) {
            clusterManager.updateNodePartitions(msg.getNodeId(), true);
            if (msg.getNodeId().equals(metadataNodeId)) {
                clusterManager.updateMetadataNode(metadataNodeId, true);
            }
            clusterManager.refreshState();
        } else {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(Level.SEVERE, msg.getNodeId() + " failed to complete startup. ", msg.getException());
            }
        }
    }

    private List<INCLifecycleTask> buildNCStartupSequence(String nodeId, SystemState state) {
        final List<INCLifecycleTask> tasks = new ArrayList<>();
        if (state == SystemState.CORRUPTED) {
            //need to perform local recovery for node partitions
            LocalRecoveryTask rt = new LocalRecoveryTask(Arrays.asList(clusterManager.getNodePartitions(nodeId))
                    .stream().map(ClusterPartition::getPartitionId).collect(Collectors.toSet()));
            tasks.add(rt);
        }
        final boolean isMetadataNode = nodeId.equals(metadataNodeId);
        if (isMetadataNode) {
            tasks.add(new MetadataBootstrapTask());
        }
        tasks.add(new ExternalLibrarySetupTask(isMetadataNode));
        tasks.add(new ReportMaxResourceIdTask());
        tasks.add(new CheckpointTask());
        tasks.add(new StartLifecycleComponentsTask());
        if (isMetadataNode) {
            tasks.add(new BindMetadataNodeTask(true));
        }
        return tasks;
    }
}