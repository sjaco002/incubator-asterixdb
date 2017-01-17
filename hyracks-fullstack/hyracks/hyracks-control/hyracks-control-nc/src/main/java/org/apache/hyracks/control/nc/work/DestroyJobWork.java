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

package org.apache.hyracks.control.nc.work;

import java.util.Map;

import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.NodeControllerService;

/**
 * destroy a permanently distributed job
 *
 */
public class DestroyJobWork extends AbstractWork {

    private final NodeControllerService ncs;
    private final JobId jobId;

    public DestroyJobWork(NodeControllerService ncs, JobId jobId) {
        this.ncs = ncs;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        Map<JobId, ActivityClusterGraph> acgMap = ncs.getActivityClusterGraphMap();
        ActivityClusterGraph acg = acgMap.get(jobId);
        if (acg == null) {
            throw new RuntimeException("Trying to destroy a job that was never distributed!");
        }
        acgMap.remove(jobId);
    }

}
