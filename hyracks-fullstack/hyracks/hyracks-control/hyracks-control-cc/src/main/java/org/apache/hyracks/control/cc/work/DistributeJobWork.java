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

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.IActivityClusterGraphGenerator;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.application.CCApplicationContext;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class DistributeJobWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final byte[] acggfBytes;
    private final JobId jobId;
    private final IResultCallback<JobId> callback;

    public DistributeJobWork(ClusterControllerService ccs, byte[] acggfBytes, JobId jobId,
            IResultCallback<JobId> callback) {
        this.jobId = jobId;
        this.ccs = ccs;
        this.acggfBytes = acggfBytes;
        this.callback = callback;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            final CCApplicationContext appCtx = ccs.getApplicationContext();
            Map<JobId, ActivityClusterGraph> acgMap = ccs.getActivityClusterGraphMap();
            Map<JobId, Set<Constraint>> acgConstaintsMap = ccs.getActivityClusterGraphConstraintsMap();
            ActivityClusterGraph entry = acgMap.get(jobId);
            Set<Constraint> constaints = acgConstaintsMap.get(jobId);
            if (entry != null || constaints != null) {
                throw new HyracksException("Trying to distribute a job with a duplicate jobId");
            }
            IActivityClusterGraphGeneratorFactory acggf =
                    (IActivityClusterGraphGeneratorFactory) DeploymentUtils.deserialize(acggfBytes, null, appCtx);
            IActivityClusterGraphGenerator acgg =
                    acggf.createActivityClusterGraphGenerator(jobId, appCtx, EnumSet.noneOf(JobFlag.class));
            ActivityClusterGraph acg = acgg.initialize();
            acgMap.put(jobId, acg);
            acgConstaintsMap.put(jobId, acgg.getConstraints());

            appCtx.notifyJobCreation(jobId, acggf);

            byte[] acgBytes = JavaSerializationUtils.serialize(acg);
            for (NodeControllerState node : ccs.getNodeMap().values()) {
                //TODO: Can we just use acggfBytes here?
                node.getNodeController().distributeJob(jobId, acgBytes);
            }

            callback.setValue(jobId);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}
