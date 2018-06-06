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
package org.apache.hyracks.control.cc.work;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.controllers.NodeParameters;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.ipc.NodeControllerRemoteProxy;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RegisterNodeWork extends SynchronizableWork {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ClusterControllerService ccs;
    private final NodeRegistration reg;
    private final int registrationId;

    public RegisterNodeWork(ClusterControllerService ccs, NodeRegistration reg, int registrationId) {
        this.ccs = ccs;
        this.reg = reg;
        this.registrationId = registrationId;
    }

    @Override
    protected void doRun() throws Exception {
        String id = reg.getNodeId();
        LOGGER.warn("Registering node: {}", id);
        NodeControllerRemoteProxy nc = new NodeControllerRemoteProxy(ccs.getCcId(),
                ccs.getClusterIPC().getReconnectingHandle(reg.getNodeControllerAddress()));
        INodeManager nodeManager = ccs.getNodeManager();
        try {
            NodeControllerState state = new NodeControllerState(nc, reg);
            nodeManager.addNode(id, state);
            IApplicationConfig cfg = state.getNCConfig().getConfigManager().getNodeEffectiveConfig(id);
            final Map<IOption, Object> ncConfiguration = new HashMap<>();
            for (IOption option : cfg.getOptions()) {
                ncConfiguration.put(option, cfg.get(option));
            }
            LOGGER.warn("Registered node: {}", id);
            NodeParameters params = new NodeParameters();
            params.setClusterControllerInfo(ccs.getClusterControllerInfo());
            params.setDistributedState(ccs.getContext().getDistributedState());
            params.setHeartbeatPeriod(ccs.getCCConfig().getHeartbeatPeriodMillis());
            params.setProfileDumpPeriod(ccs.getCCConfig().getProfileDumpPeriod());
            params.setRegistrationId(registrationId);
            LOGGER.warn("sending registration response to node {}", id);
            nc.sendRegistrationResult(params, null);
            LOGGER.warn("notifying node {} joined", id);
            ccs.getContext().notifyNodeJoin(id, ncConfiguration);
        } catch (Exception e) {
            LOGGER.error("Node {} registration failed", id, e);
            nodeManager.removeNode(id);
            nc.sendRegistrationResult(null, e);
        }
    }
}
