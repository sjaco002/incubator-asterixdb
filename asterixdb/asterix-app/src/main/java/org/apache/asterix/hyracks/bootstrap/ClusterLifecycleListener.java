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
package org.apache.asterix.hyracks.bootstrap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IClusterEventsSubscriber;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IClusterManagementWorkResponse;
import org.apache.asterix.common.api.IClusterManagementWorkResponse.Status;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.cluster.AddNodeWork;
import org.apache.asterix.metadata.cluster.AddNodeWorkResponse;
import org.apache.asterix.metadata.cluster.ClusterManagerProvider;
import org.apache.asterix.metadata.cluster.RemoveNodeWork;
import org.apache.asterix.metadata.cluster.RemoveNodeWorkResponse;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.hyracks.api.application.IClusterLifecycleListener;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.exceptions.HyracksException;

public class ClusterLifecycleListener implements IClusterLifecycleListener {

    private static final Logger LOGGER = Logger.getLogger(ClusterLifecycleListener.class.getName());

    private static final LinkedBlockingQueue<Set<IClusterManagementWork>> workRequestQueue = new LinkedBlockingQueue<Set<IClusterManagementWork>>();

    private static ClusterWorkExecutor eventHandler = new ClusterWorkExecutor(workRequestQueue);

    private static List<IClusterManagementWorkResponse> pendingWorkResponses = new ArrayList<IClusterManagementWorkResponse>();

    public static ClusterLifecycleListener INSTANCE = new ClusterLifecycleListener();

    private ClusterLifecycleListener() {
        Thread t = new Thread(eventHandler);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting cluster event handler");
        }
        t.start();
    }

    @Override
    public void notifyNodeJoin(String nodeId, Map<IOption, Object> ncConfiguration) throws HyracksException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("NC: " + nodeId + " joined");
        }
        ClusterStateManager.INSTANCE.addNCConfiguration(nodeId, ncConfiguration);

        //if metadata node rejoining, we need to rebind the proxy connection when it is active again.
        if (!ClusterStateManager.INSTANCE.isMetadataNodeActive()) {
            MetadataManager.INSTANCE.rebindMetadataNode();
        }

        Set<String> nodeAddition = new HashSet<String>();
        nodeAddition.add(nodeId);
        updateProgress(ClusterEventType.NODE_JOIN, nodeAddition);
        Set<IClusterEventsSubscriber> subscribers =
                ClusterManagerProvider.getClusterManager().getRegisteredClusterEventSubscribers();
        Set<IClusterManagementWork> work = new HashSet<IClusterManagementWork>();
        for (IClusterEventsSubscriber sub : subscribers) {
            Set<IClusterManagementWork> workRequest = sub.notifyNodeJoin(nodeId);
            work.addAll(workRequest);
        }
        if (!work.isEmpty()) {
            executeWorkSet(work);
        }

    }

    @Override
    public void notifyNodeFailure(Collection<String> deadNodeIds) throws HyracksException {
        for (String deadNode : deadNodeIds) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("NC: " + deadNode + " left");
            }
            ClusterStateManager.INSTANCE.removeNCConfiguration(deadNode);

            //if metadata node failed, we need to rebind the proxy connection when it is active again
            if (!ClusterStateManager.INSTANCE.isMetadataNodeActive()) {
                MetadataManager.INSTANCE.rebindMetadataNode();
            }
        }
        updateProgress(ClusterEventType.NODE_FAILURE, deadNodeIds);
        Set<IClusterEventsSubscriber> subscribers =
                ClusterManagerProvider.getClusterManager().getRegisteredClusterEventSubscribers();
        Set<IClusterManagementWork> work = new HashSet<IClusterManagementWork>();
        for (IClusterEventsSubscriber sub : subscribers) {
            Set<IClusterManagementWork> workRequest = sub.notifyNodeFailure(deadNodeIds);
            work.addAll(workRequest);
        }
        if (!work.isEmpty()) {
            executeWorkSet(work);
        }
    }

    private void updateProgress(ClusterEventType eventType, Collection<String> nodeIds) {
        List<IClusterManagementWorkResponse> completedResponses = new ArrayList<IClusterManagementWorkResponse>();
        boolean isComplete = false;
        for (IClusterManagementWorkResponse resp : pendingWorkResponses) {
            switch (eventType) {
                case NODE_FAILURE:
                    isComplete = ((RemoveNodeWorkResponse) resp).updateProgress(nodeIds);
                    if (isComplete) {
                        resp.setStatus(Status.SUCCESS);
                        resp.getWork().getSourceSubscriber().notifyRequestCompletion(resp);
                        completedResponses.add(resp);
                    }
                    break;

                case NODE_JOIN:
                    isComplete = ((AddNodeWorkResponse) resp).updateProgress(nodeIds.iterator().next());
                    if (isComplete) {
                        resp.setStatus(Status.SUCCESS);
                        resp.getWork().getSourceSubscriber().notifyRequestCompletion(resp);
                        completedResponses.add(resp);
                    }
                    break;
            }
        }
        pendingWorkResponses.removeAll(completedResponses);
    }

    private void executeWorkSet(Set<IClusterManagementWork> workSet) {
        int nodesToAdd = 0;
        Set<String> nodesToRemove = new HashSet<String>();
        Set<AddNodeWork> nodeAdditionRequests = new HashSet<AddNodeWork>();
        Set<IClusterManagementWork> nodeRemovalRequests = new HashSet<IClusterManagementWork>();
        for (IClusterManagementWork w : workSet) {
            switch (w.getClusterManagementWorkType()) {
                case ADD_NODE:
                    if (nodesToAdd < ((AddNodeWork) w).getNumberOfNodesRequested()) {
                        nodesToAdd = ((AddNodeWork) w).getNumberOfNodesRequested();
                    }
                    nodeAdditionRequests.add((AddNodeWork) w);
                    break;
                case REMOVE_NODE:
                    nodesToRemove.addAll(((RemoveNodeWork) w).getNodesToBeRemoved());
                    nodeRemovalRequests.add(w);
                    RemoveNodeWorkResponse response = new RemoveNodeWorkResponse((RemoveNodeWork) w,
                            Status.IN_PROGRESS);
                    pendingWorkResponses.add(response);
                    break;
            }
        }

        List<String> addedNodes = new ArrayList<String>();
        String asterixInstanceName = ClusterProperties.INSTANCE.getCluster().getInstanceName();
        for (int i = 0; i < nodesToAdd; i++) {
            Node node = ClusterStateManager.INSTANCE.getAvailableSubstitutionNode();
            if (node != null) {
                try {
                    ClusterManagerProvider.getClusterManager().addNode(node);
                    addedNodes.add(asterixInstanceName + "_" + node.getId());
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Added NC at:" + node.getId());
                    }
                } catch (AsterixException e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to add NC at:" + node.getId());
                    }
                    e.printStackTrace();
                }
            } else {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unable to add NC: no more available nodes");
                }

            }
        }

        for (AddNodeWork w : nodeAdditionRequests) {
            int n = w.getNumberOfNodesRequested();
            List<String> nodesToBeAddedForWork = new ArrayList<String>();
            for (int i = 0; i < n && i < addedNodes.size(); i++) {
                nodesToBeAddedForWork.add(addedNodes.get(i));
            }
            if (nodesToBeAddedForWork.isEmpty()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Unable to satisfy request by " + w);
                }
                AddNodeWorkResponse response = new AddNodeWorkResponse(w, nodesToBeAddedForWork);
                response.setStatus(Status.FAILURE);
                w.getSourceSubscriber().notifyRequestCompletion(response);

            } else {
                AddNodeWorkResponse response = new AddNodeWorkResponse(w, nodesToBeAddedForWork);
                pendingWorkResponses.add(response);
            }
        }

    }
}
